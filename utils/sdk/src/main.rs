pub mod ec2;
pub mod ip;
pub mod keypair;
pub mod security_rules;
pub mod ssm;
pub mod utils;
pub mod vpc;

use aws_config::{meta::region::RegionProviderChain, Region};
use aws_sdk_ec2::{types::InstanceType, Client as EC2Client};
use ip::get_public_ip;
use keypair::{create_key_pair, save_private_key};
use ssm::get_latest_ami_id;
use std::{error::Error, time::Duration};
use utils::{setup_connection, EC2Impl};
use vpc::{
    add_igw_route_if_not_exists, attach_internet_gateway_if_not_exists, enable_auto_assign_ip,
};

pub enum Arch {
    ARM,
    X86_64,
}

const INSTANCE_NAME: &str = "mvp-server";
const KEY_PAIR_NAME: &str = "mvp-key-pair-server";
const IS_SPOT_INSTANCE: bool = true;

// Depends on Instance (d3en is x86_64)
const ARCH: Arch = Arch::X86_64;
const INSTANCE_TYPE: InstanceType = InstanceType::D3en4xlarge;
const SECURITY_GROUP_NAME: &str = "mvp-security-group";
const VPC_NAME: &str = "mvp-vpc";
const SUBNET_NAME: &str = "mvp-subnet";

const SSM_ROLE_NAME: &str = "EC2SSMRole";

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let region_provider =
        RegionProviderChain::default_provider().or_else(Region::new("eu-north-1"));
    let config = aws_config::from_env().region(region_provider).load().await;
    let ec2_client = EC2Client::new(&config);
    let ec2 = EC2Impl::new(ec2_client);

    run(&ec2).await?;
    tokio::time::sleep(Duration::from_secs(15)).await;
    setup_connection(&ec2, INSTANCE_NAME).await?;
    Ok(())
}

async fn run(ec2: &EC2Impl) -> Result<(), Box<dyn Error>> {
    println!("Creating VPC & Subnet...");
    let vpc = ec2.create_vpc_if_not_exists(VPC_NAME).await?;
    let subnet = ec2
        .create_subnet_if_not_exists(SUBNET_NAME, vpc.vpc_id().unwrap())
        .await?;
    let vpc_id = vpc.vpc_id().unwrap();
    let subnet_id = subnet.subnet_id().unwrap();

    println!("Creating Internet Gateway & Route in Route Table...");
    let igw_id = attach_internet_gateway_if_not_exists(&ec2, vpc_id).await?;
    add_igw_route_if_not_exists(&ec2, vpc_id, &igw_id).await?;

    println!("Enabling Ip Auto Assign...");
    enable_auto_assign_ip(&ec2, subnet_id).await?;

    println!("Creating Security Group...");
    let security_group = ec2
        .create_security_group_if_not_exists(
            SECURITY_GROUP_NAME,
            "Mvp Security Group for SSH",
            vpc_id,
        )
        .await?;

    println!("Creating Key Pair...");
    let (key_pair_info, private_key) = create_key_pair(&ec2, KEY_PAIR_NAME).await?;

    println!("Saving private Key...");
    save_private_key(KEY_PAIR_NAME, &private_key)?;

    println!("Open Ports 22 & 80...");
    let my_cidr = format!("{}/32", get_public_ip().await?);
    let group_id = security_group.group_id().unwrap();
    ec2.add_ingress_rule_if_not_exists(group_id, &my_cidr, 22)
        .await?;
    ec2.add_ingress_rule_if_not_exists(group_id, &my_cidr, 80)
        .await?;

    println!("Allowing all traffic within the subnet...");
    let subnet_cidr = "10.0.0.0/16";
    ec2.add_ingress_rule_if_not_exists(group_id, subnet_cidr, -1)
        .await?;

    println!("Creating Instance...");
    let ami_id = get_latest_ami_id(ARCH).await?;
    let instance_id = ec2
        .create_instance(
            ami_id.as_str(),
            INSTANCE_TYPE,
            &key_pair_info,
            vec![&security_group],
            INSTANCE_NAME,
            subnet_id,
            Some(SSM_ROLE_NAME),
        )
        .await?;
    println!("Launched EC2 instance with ID: {}", instance_id);
    // ec2.wait_for_instance_ready(&instance_id, Some(Duration::from_secs(120)))
    //     .await?;
    Ok(())
}
