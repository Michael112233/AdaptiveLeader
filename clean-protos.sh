#!/bin/bash
# How to use:
# bash clean-protos.sh deleteAll (delete all generated .pb.go and _grpc.pb.go files)

deleteAll() {
  echo "Deleting all generated .pb.go and _grpc.pb.go files..."
  find ./proto -name "*.pb.go" -type f -delete
  find ./proto -name "*_grpc.pb.go" -type f -delete
  echo "All generated files deleted."
}

# Execute corresponding function based on arguments
case "${1:-}" in
  "deleteAll")
    deleteAll
    ;;
  *)
    # Default: execute deleteAll
    deleteAll
    ;;
esac
