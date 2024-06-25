import logging
from multiprocessing import Pool

from . import attributes, cleanup, download, inputs, merge, outputs, polygons, template
from .utils import ADM_LEVELS, DOWNLOAD, get_adm0_file, get_admx_files

logger = logging.getLogger(__name__)
funcs = [attributes.main, polygons.main]


def src_admx(func):
    results = []
    pool = Pool()
    for file in get_admx_files():
        result = pool.apply_async(func, args=[file])
        results.append(result)
    pool.close()
    pool.join()
    for result in results:
        result.get()


def dest_admx(func):
    results = []
    pool = Pool()
    for lvl in range(ADM_LEVELS, 0, -1):
        result = pool.apply_async(func, args=[lvl, get_admx_files()])
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
        src_admx(inputs.admx)
        src_admx(attributes.main)
        for file in get_admx_files():
            polygons.main(file)
        template.main(get_admx_files())
        dest_admx(merge.main)
        src_admx(cleanup.admx)
        dest_admx(outputs.main)
        cleanup.adm0()
        dest_admx(cleanup.dest_admx)
    logger.info("finished")
