set -e

git clone https://github.com/sstephenson/bats
mkdir -p bats/build
bats/install.sh bats/build

echo "$(pwd)/bats/build/bin" >> $GITHUB_PATH