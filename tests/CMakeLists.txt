# Copyright (C) 2022 Toitware ApS.
# Use of this source code is governed by a Zero-Clause BSD license that can
# be found in the tests/LICENSE file.

file(GLOB TESTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_test.toit")

set(TOIT_EXEC "toit.run" CACHE FILEPATH "The executable used to run the tests")
set(TPKG_EXEC "toit.pkg" CACHE FILEPATH "The executable used to install the packages")
set(TEST_TIMEOUT 40 CACHE STRING "The maximal amount of time each test is allowed to run")
set(WITH_MOSQUITTO TRUE CACHE BOOL "Whether the tests should run with mosquitto")

add_custom_target(
  "install-pkgs"
  COMMAND "${TPKG_EXEC}" install
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

include(ProcessorCount)
ProcessorCount(NUM_CPU)

add_custom_target(
  check
  COMMAND ${CMAKE_CTEST_COMMAND} -j${NUM_CPU} -T test --output-on-failure
  USES_TERMINAL
)

if (${WITH_MOSQUITTO})
  set(MOSQUITTO_FLAG "--mosquitto")
endif()

foreach(file ${TESTS})
  add_test(
    NAME ${file}
    COMMAND "${TOIT_EXEC}" "${file}" ${MOSQUITTO_FLAG}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
  set_tests_properties(${file} PROPERTIES TIMEOUT ${TEST_TIMEOUT})
endforeach()
