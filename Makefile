docker-image:
	nix build .\#eckd-docker

docker-load: docker-image
	docker load -i result

docker-push: docker-load
	docker push jeffas/etcd:latest
