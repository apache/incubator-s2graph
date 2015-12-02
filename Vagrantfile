# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = "s2graph/base-box"

  # Create a private network, which allows host-only access to the machine using a specific IP.
  config.vm.network "private_network", ip: "192.168.15.166"

  # Hadoop web UI ports
  config.vm.network :forwarded_port, guest: 50070, host: 50171
  config.vm.network :forwarded_port, guest: 50075, host: 50176
  config.vm.network :forwarded_port, guest: 50090, host: 50191
  # HBase web UI ports
  config.vm.network :forwarded_port, guest: 60010, host: 60111
  config.vm.network :forwarded_port, guest: 60030, host: 60131
  # Thrift
  config.vm.network :forwarded_port, guest: 9090, host: 9191
  # ZooKeeper
  config.vm.network :forwarded_port, guest: 2181, host: 2282
  # MySQL
  config.vm.network :forwarded_port, guest: 3306, host: 3307 
  # Play
  config.vm.network :forwarded_port, guest: 9000, host: 9000

  config.ssh.username = 'vagrant'
	config.ssh.password = 'vagrant'

  # Share an additional folder to the guest VM. The first argument is the path on the host to the actual folder.
  # The second argument is the path on the guest to mount the folder.
  config.vm.synced_folder "./", "/home/vagrant/s2graph"

  # increase available memory
  config.vm.provider :virtualbox do |vb|
     vb.customize ["modifyvm", :id, "--memory", "2048"]
  end
end
