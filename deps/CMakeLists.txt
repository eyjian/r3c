# Writed by yijian (eyjian@qq.com)

set (HIREDIS hiredis-0.14.0)

exec_program(
    tar
    ${CMAKE_CURRENT_SOURCE_DIR}
    ARGS
    xzf ${HIREDIS}.tar.gz
    RETURN_VALUE errcode
)
if (errcode)
    return ()
endif ()

exec_program(
    make
    ${CMAKE_CURRENT_SOURCE_DIR}/${HIREDIS}
    ARGS
    install PREFIX=/usr/local/${HIREDIS}
    RETURN_VALUE errcode
)
if (errcode)
    return ()
endif ()

exec_program(
    ln
    ARGS
    -s /usr/local/${HIREDIS} /usr/local/hiredis
    RETURN_VALUE errcode
)
if (errcode)
    return ()
endif ()
