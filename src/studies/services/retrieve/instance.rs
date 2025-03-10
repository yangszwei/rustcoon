use crate::config::AppConfig;
use crate::studies::error::StudiesServiceError;
use crate::studies::models::instance;
use crate::studies::services::utils::find_instances;
use crate::utils::multipart;
use std::path::PathBuf;

// Retrieve instances matching the filter in a multipart response.
pub async fn instance(
    config: &AppConfig,
    db: &sqlx::AnyPool,
    filter: &instance::SearchInstanceDto,
) -> Result<multipart::Related, StudiesServiceError> {
    let mut response = multipart::Related::new(
        multipart::RelatedConfig::new(multipart::random_boundary())
            .map_err(|err| StudiesServiceError::Other(err.into()))?
            .root_type("application/dicom"),
    );

    // Find all SOP instances that match the filter
    let sop_instances = find_instances(db, &filter.clone()).await?;

    // Iterate over all the found SOP instances and add each to the multipart response
    for sop_instance in sop_instances {
        let file_path = PathBuf::from(&config.storage.path)
            .join(&sop_instance.path)
            .join("image.dcm");

        // Check if the file exists
        if file_path.try_exists().is_err() {
            return Err(StudiesServiceError::NotFound);
        }

        // Read the file data
        let file_data = tokio::fs::read(&file_path)
            .await
            .map_err(|err| StudiesServiceError::FileReadFailure(err.into()))?;

        // Add the file data as a part to the multipart response
        response.add_part(multipart::Part::new("application/dicom", file_data));
    }

    // Return the MultipartRelatedResponseBuilder, which can be used to build the final response
    Ok(response)
}
