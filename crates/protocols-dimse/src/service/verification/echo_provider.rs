use dicom_dictionary_std::uids;

use crate::context::AssociationContext;
use crate::error::DimseError;
use crate::service::verification::{CEchoRequest, CEchoResponse};
use crate::service::{
    CommandField, DescribedServiceClassProvider, ServiceBinding, ServiceClassProvider,
};

/// Verification SOP Class (C-ECHO) provider.
#[derive(Debug, Default)]
pub struct VerificationServiceProvider;

impl VerificationServiceProvider {
    pub const SOP_CLASS_UID: &'static str = uids::VERIFICATION;
}

impl ServiceClassProvider for VerificationServiceProvider {
    fn handle(&self, ctx: &mut AssociationContext) -> Result<(), DimseError> {
        let request = CEchoRequest::from_command(&ctx.read_command()?)?;
        let response = CEchoResponse::success_for(&request).to_command_object();
        ctx.send_command_object(request.presentation_context_id, &response)?;
        Ok(())
    }
}

impl DescribedServiceClassProvider for VerificationServiceProvider {
    fn bindings(&self) -> &[ServiceBinding] {
        static BINDINGS: [ServiceBinding; 1] = [ServiceBinding::new(
            CommandField::CEchoRq,
            VerificationServiceProvider::SOP_CLASS_UID,
        )];
        &BINDINGS
    }
}

#[cfg(test)]
mod tests {
    use dicom_dictionary_std::uids;

    use super::VerificationServiceProvider;
    use crate::service::{CommandField, DescribedServiceClassProvider};

    #[test]
    fn bindings_declare_c_echo_for_verification_uid() {
        let provider = VerificationServiceProvider;
        let bindings = provider.bindings();

        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].command_field, CommandField::CEchoRq);
        assert_eq!(bindings[0].sop_class_uid.as_ref(), uids::VERIFICATION);
    }
}
