cluster:
	ansible-playbook -i inventory/caelum/inventory.ini --become --become-user=root cluster.yml

reset:
	ansible-playbook -i inventory/caelum/inventory.ini --become --become-user=root reset.yml

kubeconfig:
	sed -i 's#    server: .*$$#    server: https://127.0.0.1:6443#' inventory/caelum/artifacts/admin.conf
	@echo "Run this to use the created kubeconfig: export KUBECONFIG=$$(pwd)/inventory/caelum/artifacts/admin.conf"

tunnel: kubeconfig
	pkill ssh
	ssh -fNT -L 6443:localhost:6443 apj39@caelum-401.cl.cam.ac.uk

mitogen:
	ansible-playbook -c local mitogen.yml -vv

clean:
	rm -rf dist/
	rm *.retry
