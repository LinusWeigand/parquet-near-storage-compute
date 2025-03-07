// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// This code is modified

use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;
use aws_sdk_ec2::error::ProvideErrorMetadata;
use aws_sdk_ec2::Client as EC2Client;

pub async fn setup_connection(client: &EC2Impl, instance_name: &str) -> Result<(), Box<dyn Error>> {
    let instance_id = client
        .get_instance_id_by_name_if_running(instance_name)
        .await?
        .unwrap();

    let ec2_ip = client.get_instance_public_ip(&instance_id).await?.unwrap();

    //Configure connect.sh
    let file_path = "./ip.sh";
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(file_path)?;
    writeln!(file, "{}", format!("export IP={}", ec2_ip))?;
    Ok(())
}

#[derive(Clone)]
pub struct EC2Impl {
    pub client: EC2Client,
}

impl EC2Impl {
    pub fn new(client: EC2Client) -> Self {
        EC2Impl { client }
    }
}

// snippet-start:[ec2.rust.ec2error.impl]
#[derive(Debug)]
pub struct EC2Error(pub String);
impl EC2Error {
    pub fn new(value: impl Into<String>) -> Self {
        EC2Error(value.into())
    }

    pub fn add_message(self, message: impl Into<String>) -> Self {
        EC2Error(format!("{}: {}", message.into(), self.0))
    }
}

impl<T: ProvideErrorMetadata> From<T> for EC2Error {
    fn from(value: T) -> Self {
        EC2Error(format!(
            "{}: {}",
            value
                .code()
                .map(String::from)
                .unwrap_or("unknown code".into()),
            value
                .message()
                .map(String::from)
                .unwrap_or("missing reason".into()),
        ))
    }
}

impl std::error::Error for EC2Error {}

impl std::fmt::Display for EC2Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
// snippet-end:[ec2.rust.ec2error.impl]
