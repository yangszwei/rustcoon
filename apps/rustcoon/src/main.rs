mod monolith;

use rustcoon_orchestration::OrchestratorError;

#[tokio::main]
async fn main() -> Result<(), OrchestratorError> {
    monolith::run().await
}
