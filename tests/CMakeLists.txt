# Copyright (C) 2022 Toitware ApS.
# Use of this source code is governed by a Zero-Clause BSD license that can
# be found in the tests/LICENSE file.

file(GLOB TESTS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*_test.toit" "*-test.toit")

set(TOIT_EXEC "toit.run${CMAKE_EXECUTABLE_SUFFIX}" CACHE FILEPATH "The executable used to run the tests")
set(TPKG_EXEC "toit.pkg${CMAKE_EXECUTABLE_SUFFIX}" CACHE FILEPATH "The executable used to install the packages")

set(TEST_TIMEOUT 40 CACHE STRING "The maximal amount of time each test is allowed to run")
set(WITH_MOSQUITTO TRUE CACHE BOOL "Whether the tests should also run with mosquitto")

add_custom_target(
  "install-pkgs"
  COMMAND "${TPKG_EXEC}" install
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
)

include(ProcessorCount)
ProcessorCount(NUM_CPU)

# The tests are doing a lot of waiting.
# Increase parallelism.
math(EXPR PARALLEL_COUNT "2 * ${NUM_CPU}")

add_custom_target(
  check
  COMMAND ${CMAKE_CTEST_COMMAND} -j${PARALLEL_COUNT} -T test --output-on-failure -C Debug
  USES_TERMINAL
)

foreach(file ${TESTS})
  add_test(
    NAME ${file}
    COMMAND "${TOIT_EXEC}" "${file}"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    )
  set_tests_properties(${file} PROPERTIES TIMEOUT ${TEST_TIMEOUT})

  if (${WITH_MOSQUITTO})
    set(test_name "${file}-mosquitto")
    add_test(
      NAME "${test_name}"
      COMMAND "${TOIT_EXEC}" "${file}" --mosquitto
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      )
    set_tests_properties(${test_name} PROPERTIES TIMEOUT ${TEST_TIMEOUT})
  endif()

endforeach()
