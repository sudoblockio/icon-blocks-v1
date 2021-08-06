echo "Starting proto to struct..."
export GOPATH=$HOME/go
export GOBIN=$GOPATH/bin
export PATH=$PATH:$GOROOT:$GOPATH:$GOBIN

protoc -I=. -I=$GOPATH/src --go_out=.. --gorm_out=engine=postgres:.. *.proto

echo "Completed proto to struct..."
