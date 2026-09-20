# Copyright (C) 2026 Toit contributors.
# Use of this source code is governed by a Zero-Clause BSD license that can
# be found in the tests/LICENSE file.

set(TOIT_EXEC "toit${CMAKE_EXECUTABLE_SUFFIX}" CACHE FILEPATH "Toit executable")
set(WITH_MOSQUITTO TRUE CACHE BOOL "Run Mosquitto interoperability tests")
file(GLOB TESTS CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} "*-test.toit")
add_custom_target(install-pkgs COMMAND "${TOIT_EXEC}" pkg install
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -j4 -C $<CONFIG> --output-on-failure
  WORKING_DIRECTORY "${CMAKE_BINARY_DIR}" USES_TERMINAL)
foreach(file ${TESTS})
  add_test(NAME ${file} COMMAND "${TOIT_EXEC}" run "${file}")
  set_tests_properties(${file} PROPERTIES TIMEOUT 20 WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
endforeach()
if(WITH_MOSQUITTO)
  find_program(MOSQUITTO mosquitto)
  find_program(MOSQUITTO_SUB mosquitto_sub)
  find_program(MOSQUITTO_PUB mosquitto_pub)
  if(MOSQUITTO AND MOSQUITTO_SUB AND MOSQUITTO_PUB)
    add_test(NAME interoperability COMMAND "${TOIT_EXEC}" run interop.toit)
    set_tests_properties(interoperability PROPERTIES TIMEOUT 30 LABELS integration
      WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")
  else()
    message(STATUS "Mosquitto broker/client tools unavailable; interoperability test disabled")
  endif()
endif()
