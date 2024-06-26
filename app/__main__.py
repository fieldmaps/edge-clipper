import logging
from math import ceil
from multiprocessing import Pool, cpu_count

from . import (
    attributes,
    cleanup,
    clip,
    dissolve,
    download,
    fill,
    inputs,
    merge,
    outputs,
)
from .utils import (
    ADM_LEVELS,
    DOWNLOAD,
    THROTTLE,
    apply_funcs,
    get_adm0_file,
    get_admx_files,
)

logger = logging.getLogger(__name__)


def src_admx(funcs):
    results = []
    pool = Pool(processes=ceil(cpu_count() / THROTTLE))
    for file in get_admx_files():
        args = [file, *funcs]
        result = pool.apply_async(apply_funcs, args=args)
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def dest_admx(func, exts):
    results = []
    pool = Pool()
    for lvl in range(ADM_LEVELS, 0, -1):
        for ext in exts:
            result = pool.apply_async(func, args=[lvl, ext, get_admx_files()])
            results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


if __name__ == "__main__":
    logger.info("starting")
    if DOWNLOAD:
        download.main()
    else:
        inputs.adm0(get_adm0_file())
        src_admx([inputs.admx, attributes.main, clip.main, dissolve.main])
        dest_admx(merge.main, [None])
        src_admx([cleanup.admx])
        fill.main(get_admx_files())
        cleanup.adm0()
        dest_admx(outputs.main, ["gpkg", "gdb"])
        dest_admx(cleanup.dest_admx, [None])
    logger.info("finished")
