use crate::utils::EC2Impl;
use aws_sdk_ec2::types::{AttributeBooleanValue, Filter, Subnet, Tag, Vpc};
use std::error::Error;

impl EC2Impl {
    pub async fn create_vpc_if_not_exists(&self, vpc_name: &str) -> Result<Vpc, Box<dyn Error>> {
        let name_filter = Filter::builder().name("tag:Name").values(vpc_name).build();
        
        let response = self.client.describe_vpcs().filters(name_filter).send().await?;
        
        if let Some(existing_vpc) = response.vpcs().get(0) {
            println!("Using existing VPC with ID: {}", existing_vpc.vpc_id().unwrap());
            return Ok(existing_vpc.clone());
        }
        
        let vpc = create_vpc(self, vpc_name).await?;
        Ok(vpc)
    }
    pub async fn create_subnet_if_not_exists(
        &self,
        subnet_name: &str,
        vpc_id: &str,
    ) -> Result<Subnet, Box<dyn Error>> {
        let name_filter = Filter::builder().name("tag:Name").values(subnet_name).build();
        
        let response = self.client.describe_subnets().filters(name_filter).send().await?;
        
        if let Some(existing_subnet) = response.subnets().get(0) {
            println!("Using existing subnet with ID: {}", existing_subnet.subnet_id().unwrap());
            return Ok(existing_subnet.clone());
        }
        
        let subnet = create_subnet(self, subnet_name, vpc_id).await?;
        Ok(subnet)
    }
}

pub async fn create_vpc(ec2: &EC2Impl, vpc_name: &str) -> Result<Vpc, Box<dyn Error>> {
    let create_vpc_output = ec2
        .client
        .create_vpc()
        .cidr_block("10.0.0.0/16")
        .send()
        .await?;

    let vpc = create_vpc_output
        .vpc()
        .ok_or("Failed to create VPC")?
        .clone();
    println!("Created VPC with ID: {}", vpc.vpc_id().unwrap());

    ec2.client
        .create_tags()
        .resources(vpc.vpc_id().unwrap())
        .tags(Tag::builder().key("Name").value(vpc_name).build())
        .send()
        .await?;

    ec2.client
        .modify_vpc_attribute()
        .vpc_id(vpc.vpc_id().unwrap())
        .enable_dns_support(AttributeBooleanValue::builder().value(true).build())
        .send()
        .await?;
    ec2.client
        .modify_vpc_attribute()
        .vpc_id(vpc.vpc_id().unwrap())
        .enable_dns_hostnames(AttributeBooleanValue::builder().value(true).build())
        .send()
        .await?;

    Ok(vpc)
}

pub async fn create_subnet(ec2: &EC2Impl, subnet_name: &str, vpc_id: &str) -> Result<Subnet, Box<dyn Error>> {
    let create_subnet_output = ec2
        .client
        .create_subnet()
        .vpc_id(vpc_id)
        .cidr_block("10.0.1.0/24")
        .send()
        .await?;

    let subnet = create_subnet_output
        .subnet()
        .ok_or("Failed to create subnet")?
        .clone();
    println!("Created Subnet with ID: {}", subnet.subnet_id().unwrap());

    ec2.client
        .create_tags()
        .resources(subnet.subnet_id().unwrap())
        .tags(Tag::builder().key("Name").value(subnet_name).build())
        .send()
        .await?;
    Ok(subnet)
}

pub async fn attach_internet_gateway_if_not_exists(
    ec2: &EC2Impl,
    vpc_id: &str,
) -> Result<String, Box<dyn Error>> {
    let describe_igws_output = ec2.client.describe_internet_gateways()
        .filters(Filter::builder().name("attachment.vpc-id").values(vpc_id).build())
        .send()
        .await?;

    if let Some(existing_igw) = describe_igws_output.internet_gateways().get(0) {
        let igw_id = existing_igw.internet_gateway_id().unwrap();
        println!("Using existing Internet Gateway with ID: {}", igw_id);
        return Ok(igw_id.to_string());
    }

    let igw_id = attach_internet_gateway(ec2, vpc_id).await?;
    Ok(igw_id.to_string())
}

pub async fn attach_internet_gateway(ec2: &EC2Impl, vpc_id: &str) -> Result<String, Box<dyn Error>> {
    let create_internet_gateway_output = ec2.client.create_internet_gateway().send().await?;
    let igw_id = create_internet_gateway_output
        .internet_gateway()
        .ok_or("Failed to create internet gateway")?
        .internet_gateway_id()
        .ok_or("No internet gateway ID")?;
    println!("Created Internet Gateway with ID: {}", igw_id);

    ec2.client
        .attach_internet_gateway()
        .internet_gateway_id(igw_id)
        .vpc_id(vpc_id)
        .send()
        .await?;
    println!("Attached Internet Gateway to VPC");

    Ok(igw_id.to_string())
}

pub async fn add_igw_route_if_not_exists(
    ec2: &EC2Impl,
    vpc_id: &str,
    igw_id: &str,
) -> Result<(), Box<dyn Error>> {
    let describe_route_tables_output = ec2.client.describe_route_tables()
        .filters(Filter::builder().name("vpc-id").values(vpc_id).build())
        .send().await?;
    let route_table = describe_route_tables_output
        .route_tables()
        .first()
        .ok_or("No route tables found for VPC")?;
    let route_table_id = route_table.route_table_id().ok_or("No route table ID found")?;

    let existing_route = route_table
        .routes()
        .iter()
        .find(|route| {
            route.destination_cidr_block() == Some("0.0.0.0/0")
                && route.gateway_id() == Some(igw_id)
        });

    if existing_route.is_some() {
        println!("Route to Internet Gateway already exists.");
        return Ok(());
    }

    add_igw_route(ec2, route_table_id, igw_id).await?;
    Ok(())
}

pub async fn add_igw_route(ec2: &EC2Impl, route_table_id: &str, igw_id: &str) -> Result<(), Box<dyn Error>> {

    ec2.client
        .create_route()
        .route_table_id(route_table_id)
        .destination_cidr_block("0.0.0.0/0")
        .gateway_id(igw_id)
        .send()
        .await?;
    println!("Added route to internet gateway");

    Ok(())
}

pub async fn enable_auto_assign_ip(ec2: &EC2Impl, subnet_id: &str) -> Result<(), Box<dyn Error>> {
    ec2.client
        .modify_subnet_attribute()
        .subnet_id(subnet_id)
        .map_public_ip_on_launch(AttributeBooleanValue::builder().value(true).build())
        .send()
        .await?;
    println!("Enabled auto-assign public IP for subnet");

    Ok(())
}

