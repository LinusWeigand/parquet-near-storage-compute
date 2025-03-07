use aws_sdk_ssm::Client as SSMClient;
use std::error::Error;

use crate::Arch;

pub async fn get_latest_ami_id(arch: Arch) -> Result<String, Box<dyn Error>> {
    let config = aws_config::load_from_env().await;
    let ssm_client = SSMClient::new(&config);

    let ami_param_name = match arch {
        Arch::ARM => "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
        Arch::X86_64 => "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
    };
    let ami_param = ssm_client
        .get_parameter()
        .name(ami_param_name)
        .send()
        .await?;

    let ami_id = ami_param.parameter.unwrap().value.unwrap();
    Ok(ami_id)
}

pub async fn setup_instance_via_ssm(
    ssm_client: &SSMClient,
    instance_id: &str,
) -> Result<(), Box<dyn Error>> {
    let binary_url = "https://github.com/LinusWeigand/Bachelor/releases/download/mvp-1.0.0/mvp";
    let wget_command = format!("wget {} -O ~/mvp", binary_url);

    let commands = vec![
        "sudo yum update -y",
        "sudo yum install -y wget",
        wget_command.as_str(),
        "chmod +x ~/mvp",
        "~/mvp",
    ];

    let document_name = "AWS-RunShellScript";

    let send_command_output = ssm_client
        .send_command()
        .instance_ids(instance_id)
        .document_name(document_name)
        .parameters("commands", commands.iter().map(|s| s.to_string()).collect())
        .send()
        .await?;

    let command_id = send_command_output
        .command()
        .and_then(|cmd| cmd.command_id())
        .ok_or("Failed to get command ID")?;

    println!("Sent SSM command with ID: {}", command_id);
    Ok(())
}
