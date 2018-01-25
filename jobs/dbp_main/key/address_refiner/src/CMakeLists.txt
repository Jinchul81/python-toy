cmake_minimum_required(VERSION 2.8)

INCLUDE_DIRECTORIES(INCLUDE)
SET(SOURCES main.cc)
ADD_DEFINITIONS(-Wall -std=c++11)
ADD_EXECUTABLE(address_refiner ${SOURCES})
#TARGET_LINK_LIBRARIES(address_refiner -lpthread -lboost_system -lboost_filesystem -lboost_regex -g -pg -fno-inline-functions)
TARGET_LINK_LIBRARIES(address_refiner -lpthread -lboost_system -lboost_filesystem -lboost_regex)
