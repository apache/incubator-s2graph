# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  # Every Vagrant virtual environment requires a box to build off of.
  config.vm.box = "ubuntu/trusty64"

  # Create a private network, which allows host-only access to the machine using a specific IP.
  config.vm.network "private_network", ip: "192.168.15.166"

  # Hadoop web UI ports
  config.vm.network :forwarded_port, guest: 50070, host: 50170
  config.vm.network :forwarded_port, guest: 50075, host: 50175
  config.vm.network :forwarded_port, guest: 50090, host: 50190
  # HBase web UI ports
  config.vm.network :forwarded_port, guest: 60010, host: 60110
  config.vm.network :forwarded_port, guest: 60030, host: 60130
  # Thrift
  config.vm.network :forwarded_port, guest: 9090, host: 9190
  # ZooKeeper
  config.vm.network :forwarded_port, guest: 2181, host: 2281
  # MySQL
  config.vm.network :forwarded_port, guest: 3306, host: 3306 
  # Play
  config.vm.network :forwarded_port, guest: 9000, host: 9000

	config.ssh.forward_agent = true

  # Share an additional folder to the guest VM. The first argument is the path on the host to the actual folder.
  # The second argument is the path on the guest to mount the folder.
  config.vm.synced_folder "./", "/home/vagrant/s2graph"

  # increase available memory
  config.vm.provider :virtualbox do |vb|
     vb.customize ["modifyvm", :id, "--memory", "4096"]
  end
  is_windows = (RbConfig::CONFIG['host_os'] =~ /mswin|mingw|cygwin/)
  if is_windows
    # Provisioning configuration for shell script.
    config.vm.provision "shell" do |sh|
      sh.privileged = false
      sh.path = "script/windows.sh"
      sh.args = "ansible/playbook.yml ansible/ansible_hosts"
    end
  else
    config.vm.provision :ansible do |ansible|
      ansible.playbook = "ansible/playbook.yml"
      ansible.inventory_path = "ansible/ansible_hosts"
      ansible.limit = "all"
      ansible.verbose = "vv"
    end
  end
end
