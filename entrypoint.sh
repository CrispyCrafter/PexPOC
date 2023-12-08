# See 
# https://www.youtube.com/watch?v=pyRwQByuFfc
# https://github.com/wjones127/arrow-ipc-bench

extract_version() {
    local filename="$1"
    local version
    version=$(grep 'version =' "$filename" | sed -E 's/.*"([0-9.-]+)".*/\1/' | sed 's/\./_/g')
    echo $version
}

mkdir -p dist 
model_version=$(extract_version model/pyproject.toml)
dist_version=$(extract_version pyproject.toml)

pex ./model/ -o dist/model_${model_version}.pex -c run --transitive --compile --compress
pex . -o dist/base_${dist_version}.pex --transitive --compile --compress

PEX_PATH=dist/base_${dist_version}.pex ./dist/model_${model_version}.pex 