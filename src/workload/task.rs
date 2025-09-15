use super::docker::{
    ContainerManager, DockerContainerManager, RestartPolicyConfig, RestartPolicyType,
};
use anyhow::Result;
use std::collections::HashMap;
use std::time::Duration;

pub async fn submit() -> Result<()> {
    // Initialize the container manager
    let container_manager = DockerContainerManager::new().await?;

    // Pull the image first
    container_manager.pull_image("nginx:latest").await?;

    // Create a new container
    let container_id = container_manager
        .create_container(
            "my-nginx-container",
            "nginx:latest",
            None,
            None,
            None,
            Some(RestartPolicyConfig {
                name: RestartPolicyType::Always,
                max_retry_count: None,
            }),
        )
        .await?;

    // Start the container
    container_manager.start_container(&container_id).await?;
    println!("Container started: {container_id}");

    // List all running containers
    let mut filters = HashMap::new();
    filters.insert("status".to_string(), vec!["running".to_string()]);

    let containers = container_manager.list_containers(Some(filters)).await?;
    for container in containers {
        println!("Running container: {} ({})", container.name, container.id);
    }

    // Stop the container after 5 seconds
    tokio::time::sleep(Duration::from_secs(5)).await;
    container_manager
        .stop_container(&container_id, Some(Duration::from_secs(10)))
        .await?;

    // Remove the container
    container_manager
        .remove_container(&container_id, false, true)
        .await?;

    Ok(())
}
