use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use crate::studies::services::utils::find_instances;
use crate::utils::dicom::Image;
use dicom_pixeldata::image::{DynamicImage, ImageFormat};
use dicom_pixeldata::PixelDecoder;
use std::io::Cursor;
use std::path::PathBuf;

/// Render the first instance matching the filter as an image.
pub async fn rendered(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    filter: instance::SearchInstanceDto,
    frame: Option<u32>,
) -> Result<Image, StudiesServiceError> {
    // Find all SOP instances that match the filter
    let sop_instances = find_instances(db, &filter).await?;

    // Use the first SOP instance
    let sop_instance = sop_instances.first().ok_or(StudiesServiceError::NotFound)?;

    let file_path = PathBuf::from(&config.storage.path)
        .join(&sop_instance.path)
        .join("image.dcm");

    // Check if the file exists
    if let Err(_) = file_path.try_exists() {
        return Err(StudiesServiceError::NotFound);
    }

    render_dicom_image(file_path, frame.unwrap_or(0), false)
        .await
        .map(|image| Image("image/jpeg", image))
        .map_err(|err| StudiesServiceError::DicomRenderError(err.into()))
}

/// Renders an image representation for the parent DICOM resource matching the filter.
pub async fn thumbnail(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    filter: instance::SearchInstanceDto,
    frame: Option<u32>,
) -> Result<Image, StudiesServiceError> {
    // Find all SOP instances that match the filter
    let sop_instances = find_instances(db, &filter).await?;

    // Use the first SOP instance
    let sop_instance = sop_instances.first().ok_or(StudiesServiceError::NotFound)?;

    let file_path = PathBuf::from(&config.storage.path)
        .join(&sop_instance.path)
        .join("image.dcm");

    // Check if the file exists
    if let Err(_) = file_path.try_exists() {
        return Err(StudiesServiceError::NotFound);
    }

    render_dicom_image(file_path, frame.unwrap_or(0), true)
        .await
        .map(|image| Image("image/jpeg", image))
        .map_err(|err| StudiesServiceError::DicomRenderError(err.into()))
}

/// Render a DICOM image from the given file path and frame number.
async fn render_dicom_image<P>(
    file_path: P,
    frame: u32,
    thumbnail: bool,
) -> Result<Vec<u8>, Box<dyn std::error::Error>>
where
    P: AsRef<std::path::Path>,
{
    let obj = dicom::object::open_file(file_path)?;

    // Decode the pixel data
    let pixel = obj.decode_pixel_data_frame(frame)?;

    // Convert the pixel data to an image
    let mut image: DynamicImage = pixel.to_dynamic_image(0)?;

    // Resize the image if it's a thumbnail
    if thumbnail {
        image = image.thumbnail(256, 256);
    }

    // Save the image to a PNG buffer
    let mut buffer = Cursor::new(Vec::new());
    image.write_to(&mut buffer, ImageFormat::Jpeg)?;

    Ok(buffer.into_inner())
}
