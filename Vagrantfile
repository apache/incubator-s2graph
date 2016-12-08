# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
