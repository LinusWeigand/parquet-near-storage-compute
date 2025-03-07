// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// This code is modified

use crate::utils::{EC2Error, EC2Impl};
use crate::IS_SPOT_INSTANCE;
use aws_sdk_ec2::client::Waiters;
use aws_sdk_ec2::types::{
    Filter, IamInstanceProfileSpecification, Instance, InstanceMarketOptionsRequest,
    InstanceStateName, InstanceType, KeyPairInfo, MarketType, SecurityGroup, Tag,
};
use aws_smithy_runtime_api::client::waiters::error::WaiterError;
use std::time::Duration;

impl EC2Impl {
    // snippet-start:[ec2.rust.create_instance.impl]
    // Modified
    pub async fn create_instance<'a>(
        &self,
        image_id: &'a str,
        instance_type: InstanceType,
        key_pair: &'a KeyPairInfo,
        security_groups: Vec<&'a SecurityGroup>,
        name: &str,
        subnet_id: &str,
        iam_instance_profile: Option<&str>,
    ) -> Result<String, EC2Error> {
        let mut instance_market_options = InstanceMarketOptionsRequest::builder();

        if IS_SPOT_INSTANCE {
            instance_market_options = instance_market_options
                .market_type(MarketType::Spot);
        }

        let instance_market_options = instance_market_options.build();

        let mut run_instances_builder = self
            .client
            .run_instances()
            .image_id(image_id)
            .instance_type(instance_type)
            .key_name(
                key_pair
                    .key_name()
                    .ok_or_else(|| EC2Error::new("Missing key name when launching instance"))?,
            )
            .set_security_group_ids(Some(
                security_groups
                    .iter()
                    .filter_map(|sg| sg.group_id.clone())
                    .collect(),
            ))
            .subnet_id(subnet_id)
            .instance_market_options(instance_market_options)
            .min_count(1)
            .max_count(1);

        if let Some(profile_name) = iam_instance_profile {
            let iam_instance_profile = IamInstanceProfileSpecification::builder()
                .name(profile_name)
                .build();
            run_instances_builder =
                run_instances_builder.iam_instance_profile(iam_instance_profile);
        }

        let run_instances = run_instances_builder.send().await?;

        if run_instances.instances().is_empty() {
            return Err(EC2Error::new("Failed to create instance"));
        }

        let instance_id = run_instances.instances()[0].instance_id().unwrap();
        let response = self
            .client
            .create_tags()
            .resources(instance_id)
            .tags(Tag::builder().key("Name").value(name).build())
            .send()
            .await;

        match response {
            Ok(_) => tracing::info!("Created {instance_id} and applied tags."),
            Err(err) => {
                tracing::info!("Error applying tags to {instance_id}: {err:?}");
                return Err(err.into());
            }
        }

        tracing::info!("Instance is created.");

        Ok(instance_id.to_string())
    }
    // snippet-end:[ec2.rust.create_instance.impl]

    // snippet-start:[ec2.rust.wait_for_instance_ready.impl]
    /// Wait for an instance to be ready and status ok (default wait 60 seconds)
    pub async fn wait_for_instance_ready(
        &self,
        instance_id: &str,
        duration: Option<Duration>,
    ) -> Result<(), EC2Error> {
        self.client
            .wait_until_instance_status_ok()
            .instance_ids(instance_id)
            .wait(duration.unwrap_or(Duration::from_secs(60)))
            .await
            .map_err(|err| match err {
                WaiterError::ExceededMaxWait(exceeded) => EC2Error(format!(
                    "Exceeded max time ({}s) waiting for instance to start.",
                    exceeded.max_wait().as_secs()
                )),
                _ => EC2Error::from(err),
            })?;
        Ok(())
    }
    // snippet-end:[ec2.rust.wait_for_instance_ready.impl]

    // snippet-start:[ec2.rust.describe_instance.impl]
    pub async fn describe_instance(&self, instance_id: &str) -> Result<Instance, EC2Error> {
        let response = self
            .client
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await?;

        let instance = response
            .reservations()
            .first()
            .ok_or_else(|| EC2Error::new(format!("No instance reservations for {instance_id}")))?
            .instances()
            .first()
            .ok_or_else(|| {
                EC2Error::new(format!("No instances in reservation for {instance_id}"))
            })?;

        Ok(instance.clone())
    }
    // snippet-end:[ec2.rust.describe_instance.impl]

    //Method added
    pub async fn get_instance_id_by_name_if_running(
        &self,
        instance_name: &str,
    ) -> Result<Option<String>, EC2Error> {
        let name_filter = Filter::builder()
            .name("tag:Name")
            .values(instance_name)
            .build();

        let response = self
            .client
            .describe_instances()
            .filters(name_filter)
            .send()
            .await?;

        if let Some(instance) = response
            .reservations()
            .iter()
            .flat_map(|res| res.instances())
            .find(|instance| {
                if let Some(state) = instance.state() {
                    if let Some(state_name) = state.name() {
                        state_name == &InstanceStateName::Running
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
        {
            if let Some(instance_id) = instance.instance_id() {
                return Ok(Some(instance_id.to_string()));
            }
        }
        Ok(None)
    }
}
