#!/usr/bin/python

import argparse
import subprocess
import yaml
import json

DOCKER_NETWORK = 'exasoldb_priv'

def docker_run(config):
    for db, properties in config.items():
        if properties.get('runIntegrationTests', False):
            if 'dockerImage' in properties:
                cmd = "docker run -d -p {port_map} --network {net} --name {name} {image}:{version}".format(
                    port_map = properties['dockerPortMapping'],
                    name = properties['dockerName'],
                    image = properties['dockerImage'],
                    version = properties['dockerImageVersion'],
                    net=DOCKER_NETWORK)
                print(cmd)
                run(cmd)
            elif 'dockerName' in properties:
                cmd = "docker start {name}".format(name = properties['dockerName'])
                print(cmd)
                run(cmd)
                cmd = "docker network connect {network} {name}".format(
                    name = properties['dockerName'],
                    network = DOCKER_NETWORK
                )
                print(cmd)
                run(cmd)

def docker_rm(config):
    for db, properties in config.items():
        if properties.get('runIntegrationTests', False):
            if 'dockerImage' in properties:
                cmd = "docker rm -f {name}".format(name = properties['dockerName'])
                print(cmd)
                run(cmd)
            elif 'dockerName' in properties:
                cmd = "docker stop {name}".format(name = properties['dockerName'])
                print(cmd)
                run(cmd)
                cmd = "docker network disconnect {network} {name}".format(
                    name = properties['dockerName'],
                    network = DOCKER_NETWORK
                )
                print(cmd)
                run(cmd)

def run(cmd):
    try: 
        p = subprocess.Popen(
            cmd,
            stdout = subprocess.PIPE, 
            stderr = subprocess.STDOUT,
            close_fds = True,
            shell = True)
        out, err = p.communicate()
        if out:
            if (p.returncode != 0):
                print(out)
            return out
        if err:
            print(err)
    finally:
        if p is not None:
            try: p.kill()
            except: pass

def replace_hosts_in(config):
    for db, properties in config.items():
        if properties.get('runIntegrationTests', False) and 'dockerName' in properties:
            container_ip = get_ip_for(properties['dockerName'])
            conn_string_with_ip = properties['dockerConnectionString'].replace(
                'DBHOST',container_ip)
            properties['dockerConnectionString'] = conn_string_with_ip
    return yaml.dump(config, default_flow_style=False)

def get_ip_for(docker_name):
    cmd = "docker network inspect {network}".format(
        network = DOCKER_NETWORK
    )
    docker_bridge_json = run(cmd)
    bridge_info = json.loads(docker_bridge_json)
    containers = bridge_info[0]['Containers']
    for id, properties in containers.items():
        if properties['Name'] == docker_name:
            return str(properties['IPv4Address'].split('/')[0])

def read_config(config_file):
    with open(config_file) as config:
        return yaml.load(config, Loader=yaml.Loader)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="YAML integration test config file")
    parser.add_argument("--run", help="run containers", action="store_true")
    parser.add_argument("--rm", help="remove containers", action="store_true")
    parser.add_argument("--add_docker_hosts", help="return config file with fixed connection strings", action="store_true")
    args = parser.parse_args()

    yaml_config = read_config(args.config)

    if args.run:
        docker_run(yaml_config)
    elif args.rm:
        docker_rm(yaml_config)
    elif args.add_docker_hosts:
        print(replace_hosts_in(yaml_config))
