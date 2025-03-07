// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// This code is modified

use std::{error::Error, fs::{create_dir_all, set_permissions, Permissions}, io::Write};
use aws_sdk_ec2::types::KeyPairInfo;
use crate::utils::{EC2Error, EC2Impl};

pub fn save_private_key(key_name: &str, private_key: &str) -> Result<(), Box<dyn Error>> {
    let home_dir = dirs::home_dir().ok_or("Could not find home directory")?;
    let key_path = home_dir.join(".ssh").join(format!("{}.pem", key_name));

    create_dir_all(key_path.parent().unwrap())?;

    let mut file = std::fs::File::create(&key_path)?;
    file.write_all(private_key.as_bytes())?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = Permissions::from_mode(0o600);
        set_permissions(&key_path, permissions)?;
    }

    println!("Private key saved to: {:?}", key_path);
    Ok(())
}

pub async fn create_key_pair(ec2: &EC2Impl, key_pair_name: &str) -> Result<(KeyPairInfo, String), Box<dyn Error>> {
    let mut key_pair_id: Option<String> = None;
    let key_pairs = ec2.list_key_pair().await?;
    for key_pair in &key_pairs {
        if key_pair.key_name == Some(key_pair_name.to_string()) {
            if let Some(id) = &key_pair.key_pair_id {
                key_pair_id = Some(id.clone());
            }
        }
        println!("Key_pair: {:?}", key_pair);
    }
    if let Some(id) = key_pair_id {
        match ec2.delete_key_pair(&id).await {
            Err(e) => println!("Key Pair does not already exist: {:?}", e),
            Ok(_) => println!("Key Pair does already exist: Deleting.."),
        };
    }
    let (key_pair_info, private_key) = ec2.create_key_pair(key_pair_name.to_string()).await?;
    Ok((key_pair_info, private_key.to_string()))
}

impl EC2Impl {
    // snippet-start:[ec2.rust.create_key.impl]
    pub async fn create_key_pair(&self, name: String) -> Result<(KeyPairInfo, String), EC2Error> {
        tracing::info!("Creating key pair {name}");
        let output = self.client.create_key_pair().key_name(name).send().await?;
        let info = KeyPairInfo::builder()
            .set_key_name(output.key_name)
            .set_key_fingerprint(output.key_fingerprint)
            .set_key_pair_id(output.key_pair_id)
            .build();
        let material = output
            .key_material
            .ok_or_else(|| EC2Error::new("Create Key Pair has no key material"))?;
        Ok((info, material))
    }
    // snippet-end:[ec2.rust.create_key.impl]

    // snippet-start:[ec2.rust.list_keys.impl]
    pub async fn list_key_pair(&self) -> Result<Vec<KeyPairInfo>, EC2Error> {
        let output = self.client.describe_key_pairs().send().await?;
        Ok(output.key_pairs.unwrap_or_default())
    }
    // snippet-end:[ec2.rust.list_keys.impl]

    // snippet-start:[ec2.rust.delete_key.impl]
    pub async fn delete_key_pair(&self, key_pair_id: &str) -> Result<(), EC2Error> {
        let key_pair_id: String = key_pair_id.into();
        tracing::info!("Deleting key pair {key_pair_id}");
        self.client
            .delete_key_pair()
            .key_pair_id(key_pair_id)
            .send()
            .await?;
        Ok(())
    }
    // snippet-end:[ec2.rust.delete_key.impl]
}
