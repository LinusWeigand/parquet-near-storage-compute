sudo yum install -y mdadm xfsprogs
sudo mdadm --create /dev/md0 --level=0 --raid-devices=8 /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1 /dev/nvme5n1 /dev/nvme6n1 /dev/nvme7n1 /dev/nvme8n1
sudo mkfs.xfs /dev/md0
sudo mkdir /mnt/raid0
sudo mount /dev/md0 /mnt/raid0
echo '/dev/md0 /mnt/raid0 xfs defaults,noatime 0 0' | sudo tee -a /etc/fstab
sudo chown ec2-user:ec2-user /mnt/raid0
