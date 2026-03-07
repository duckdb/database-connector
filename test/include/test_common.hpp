#pragma once

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "catch.hpp"

#define REQUIRE_THREAD(a) if (!(a)) { std::cerr << "REQUIRE_THREAD failure: " << __FILE__ << ":" << __LINE__ << std::endl; std::exit(1); }
