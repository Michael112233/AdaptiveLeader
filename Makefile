GO_CMD=go
PKG_PATH=github.com/Arman17Babaei/pbft/cmd/load_test

clean:
	bash clean-protos.sh deleteAll
	docker stop $(docker ps -q)
	docker rm $(docker ps -q)

compile:
	bash compile-protos.sh compileAll

down:
	docker stop $(docker ps -q)
	docker rm $(docker ps -q)

up:
	bash compile-protos.sh
	docker compose up -d

test:
	go run github.com/Arman17Babaei/pbft/cmd/load_test

test-raft:
	@echo "Generating raft mock files..."
	@export PATH=$$PATH:$$(go env GOPATH)/bin
	@cd pbft/leader_election/raft && go generate
	@echo "Running raft tests..."
	go test github.com/Arman17Babaei/pbft/pbft/leader_election/raft

test-paxos:
	@echo "Generating paxos mock files..."
	@export PATH=$$PATH:$$(go env GOPATH)/bin
	@cd pbft/leader_election/paxos && go generate
	@echo "Running paxos tests..."
	go test github.com/Arman17Babaei/pbft/pbft/leader_election/paxos

generate:
	@echo "Generating all mock files..."
	@export PATH=$$PATH:$$(go env GOPATH)/bin
	@cd pbft/leader_election/paxos && go generate
	@cd pbft/leader_election/raft && go generate
	@cd pbft && go generate
	@echo "Mock files generated successfully"

start:
	docker compose up -d