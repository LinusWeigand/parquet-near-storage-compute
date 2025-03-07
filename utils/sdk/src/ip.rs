// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// This code is modified

use std::error::Error;

use crate::utils::{EC2Impl, EC2Error};

pub async fn get_public_ip() -> Result<String, Box<dyn Error>> {
    let response = reqwest::get("https://api.ipify.org").await?;
    let ip_address = response.text().await?;
    Ok(ip_address)
}

impl EC2Impl {
    pub async fn get_instance_public_ip(
        &self,
        instance_id: &str,
    ) -> Result<Option<String>, EC2Error> {
        let response = self
            .client
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await?;

        let reservations = response.reservations();
        if let Some(instance) = reservations
            .iter()
            .flat_map(|r| r.instances())
            .find(|i| i.instance_id().unwrap() == instance_id)
        {
            return Ok(instance.public_ip_address().map(|ip| ip.to_string()));
        }

        Err(EC2Error::new(format!(
            "No public IP found for instance {}",
            instance_id
        )))
    }
}
