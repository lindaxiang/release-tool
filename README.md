# release-tool
The tool is used to generate mirna release datasets

    # install pipsi
    curl https://raw.githubusercontent.com/mitsuhiko/pipsi/master/get-pipsi.py | python

    # clone this tool
    git clone git@github.com:lindaxiang/release-tool.git

    # clone the annotations
    git clone git@github.com:ICGC-TCGA-PanCancer/pcawg-operations.git

    # install this tool
    cd release-tool
    pipsi install --editable .

    # run the tool
    release_tool build

    # output data is under
    examples/mirna_release/output
