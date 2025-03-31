use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use crate::studies::services::utils::find_instances;
use crate::utils::multipart;
use dicom::encoding::adapters::PixelDataObject;
use dicom::object::file::ReadPreamble;
use dicom::object::OpenFileOptions;
use std::path::PathBuf;

fn load_dicom_object(
    file_path: PathBuf,
) -> Result<dicom::object::DefaultDicomObject, StudiesServiceError> {
    if file_path.try_exists().is_err() {
        return Err(StudiesServiceError::NotFound);
    }

    OpenFileOptions::new()
        .read_preamble(ReadPreamble::Always)
        .open_file(file_path)
        .map_err(|err| StudiesServiceError::FileReadFailure(err.into()))
}

fn extract_frames_from_raw_pixel_data(
    raw: &dicom::encoding::adapters::RawPixelData,
    frame_index: Option<usize>,
) -> Result<Vec<Vec<u8>>, StudiesServiceError> {
    let offsets = &raw.offset_table;
    let full_data = raw.fragments.iter().flatten().copied().collect::<Vec<u8>>();

    let fragments = match frame_index {
        Some(index) => {
            if !offsets.is_empty() {
                let start = *offsets.get(index).ok_or_else(|| {
                    StudiesServiceError::Other(format!("Frame {} offset not found", index).into())
                })? as usize;

                let end = offsets
                    .get(index + 1)
                    .map(|x| *x as usize)
                    .unwrap_or(full_data.len());

                let slice = full_data
                    .get(start..end)
                    .ok_or_else(|| StudiesServiceError::Other("Invalid frame range".into()))?
                    .to_vec();

                Ok(vec![slice])
            } else {
                let frame = raw.fragments.get(index).ok_or_else(|| {
                    StudiesServiceError::Other(format!("Frame {} not found", index).into())
                })?;
                Ok(vec![frame.clone()])
            }
        }
        None => {
            if !offsets.is_empty() {
                let mut all = Vec::with_capacity(offsets.len());

                for idx in 0..offsets.len() {
                    let start = offsets[idx] as usize;
                    let end = if idx + 1 < offsets.len() {
                        offsets[idx + 1] as usize
                    } else {
                        full_data.len()
                    };

                    let slice = full_data
                        .get(start..end)
                        .ok_or_else(|| {
                            StudiesServiceError::Other(format!("Frame {} out of range", idx).into())
                        })?
                        .to_vec();

                    all.push(slice);
                }

                Ok(all)
            } else {
                Ok(raw.fragments.iter().cloned().collect())
            }
        }
    };

    fragments
}

pub async fn pixeldata(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    filter: &instance::SearchInstanceDto,
    frame_index: Option<usize>,
) -> Result<multipart::Related, StudiesServiceError> {
    let mut related = multipart::Related::new(
        multipart::RelatedConfig::new(multipart::random_boundary())
            .map_err(|err| StudiesServiceError::Other(err.into()))?,
    );

    for (i, sop_instance) in find_instances(db, filter).await?.into_iter().enumerate() {
        let file_path = PathBuf::from(&config.storage.path)
            .join(&sop_instance.path)
            .join("image.dcm");

        let obj = load_dicom_object(file_path)?;
        let raw = obj
            .raw_pixel_data()
            .ok_or_else(|| StudiesServiceError::Other("Missing raw pixel data".into()))?;

        let fragments = extract_frames_from_raw_pixel_data(&raw, frame_index)?;

        for (j, frame) in fragments.into_iter().enumerate() {
            let part = multipart::Part::new("application/octet-stream", frame).with_id(format!(
                "image{}_frame{}",
                i + 1,
                j + 1
            ));
            related.add_part(part);
        }
    }

    Ok(related)
}
