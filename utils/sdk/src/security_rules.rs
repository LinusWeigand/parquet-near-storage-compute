// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// This code is modified

use crate::{utils::EC2Error, utils::EC2Impl};
use aws_sdk_ec2::types::{Filter, IpPermission, IpRange, SecurityGroup};

impl EC2Impl {
    pub async fn add_ingress_rule_if_not_exists(
        &self,
        group_id: &str,
        cidr_ip: &str,
        port: i32,
    ) -> Result<(), EC2Error> {
        return match self
            .authorize_security_group_ingress(group_id, cidr_ip, port)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                let error_message = e.to_string();

                if error_message.contains("InvalidPermission.Duplicate") {
                    println!("The rule already exists. Skipping...");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        };
    }

    // snippet-start:[ec2.rust.authorize_security_group_ssh_ingress.impl]
    /// Add an ingress rule to a security group explicitly allowing IPv4 address
    /// as {ip}/32 over TCP port 22.
    // Modified
    pub async fn authorize_security_group_ingress(
        &self,
        group_id: &str,
        cidr_ip: &str,
        port: i32,
    ) -> Result<(), EC2Error> {
        tracing::info!("Authorizing ingress for security group {group_id}");
        let permission = match port {
            -1 => IpPermission::builder()
                .ip_protocol("-1")
                .ip_ranges(IpRange::builder().cidr_ip(cidr_ip).build())
                .build(),
            _ => IpPermission::builder()
                .ip_protocol("tcp")
                .from_port(port)
                .to_port(port)
                .ip_ranges(IpRange::builder().cidr_ip(cidr_ip).build())
                .build(),
        };
        self.client
            .authorize_security_group_ingress()
            .group_id(group_id)
            .set_ip_permissions(Some(vec![permission]))
            .send()
            .await?;
        Ok(())
    }
    // snippet-end:[ec2.rust.authorize_security_group_ssh_ingress.impl]

    //Method added
    pub async fn create_security_group_if_not_exists(
        &self,
        group_name: &str,
        description: &str,
        vpc_id: &str,
    ) -> Result<SecurityGroup, EC2Error> {
        let existing_group = self.describe_security_group_by_name(group_name).await?;

        if let Some(group) = existing_group {
            println!(
                "Security group '{}' already exists: {:#?}",
                group_name, group
            );
            return Ok(group);
        }

        let security_group = self
            .create_security_group(group_name, description, vpc_id)
            .await?;
        Ok(security_group)
    }

    //Method added
    pub async fn describe_security_group_by_name(
        &self,
        group_name: &str,
    ) -> Result<Option<SecurityGroup>, EC2Error> {
        let name_filter = Filter::builder()
            .name("group-name")
            .values(group_name)
            .build();

        let describe_output = self
            .client
            .describe_security_groups()
            .filters(name_filter)
            .send()
            .await?;

        let groups = describe_output.security_groups.unwrap_or_default();
        if groups.is_empty() {
            Ok(None)
        } else {
            Ok(Some(groups[0].clone()))
        }
    }

    // snippet-start:[ec2.rust.create_security_group.impl]
    // Modified
    pub async fn create_security_group(
        &self,
        name: &str,
        description: &str,
        vpc_id: &str,
    ) -> Result<SecurityGroup, EC2Error> {
        tracing::info!("Creating security group {name}");
        let create_output = self
            .client
            .create_security_group()
            .group_name(name)
            .description(description)
            .vpc_id(vpc_id)
            .send()
            .await
            .map_err(EC2Error::from)?;

        let group_id = create_output
            .group_id
            .ok_or_else(|| EC2Error::new("Missing security group id after creation"))?;

        let group = self
            .describe_security_group(&group_id)
            .await?
            .ok_or_else(|| {
                EC2Error::new(format!("Could not find security group with id {group_id}"))
            })?;

        tracing::info!("Created security group {name} as {group_id}");

        Ok(group)
    }
    // snippet-end:[ec2.rust.create_security_group.impl]

    pub async fn describe_security_group(
        &self,
        group_id: &str,
    ) -> Result<Option<SecurityGroup>, EC2Error> {
        let group_id: String = group_id.into();
        let describe_output = self
            .client
            .describe_security_groups()
            .group_ids(&group_id)
            .send()
            .await?;

        let mut groups = describe_output.security_groups.unwrap_or_default();

        match groups.len() {
            0 => Ok(None),
            1 => Ok(Some(groups.remove(0))),
            _ => Err(EC2Error::new(format!(
                "Expected single group for {group_id}"
            ))),
        }
    }
    // snippet-end:[ec2.rust.describe_security_group.impl]

    // snippet-start:[ec2.rust.delete_security_group.impl]
    pub async fn delete_security_group(&self, group_id: &str) -> Result<(), EC2Error> {
        tracing::info!("Deleting security group {group_id}");
        self.client
            .delete_security_group()
            .group_id(group_id)
            .send()
            .await?;
        Ok(())
    }
    // snippet-end:[ec2.rust.delete_security_group.impl]
}
