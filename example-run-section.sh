export CGO_CFLAGS="$CGO_CFLAGS -I$DRMAA1_INC_PATH -I$DRMAA2_INC_PATH"
export CGO_LDFLAGS="$CGO_LDFLAGS -L$DRMAA1_LIB_PATH -L$DRMAA2_LIB_PATH"

./dmgservice \
    -A tem -jobName dmg \
    -sessionName dmg \
    -sectionProcessor drmaa2 \
    -dmgProcessor drmaa2 \
    dmgSection \
    -serverAddress "" \
    -verbose \
    -sections 4 \
    -config config.json -config config.local.json \
    -pixels /nobackup/flyTEM/rendered_boxes/FAFB00/v12_align_tps/8192x8192/0/iGrid/9.0.iGrid \
    -labels /nobackup/flyTEM/rendered_boxes/FAFB00/v12_align_tps/8192x8192-label/0/iGrid/9.0.iGrid \
    -threads 16 \
    -temp /scratch/goinac \
    -targetDir outputDir
