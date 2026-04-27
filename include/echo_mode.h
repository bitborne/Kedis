#ifndef ECHO_MODE_H
#define ECHO_MODE_H

#include "kvs_network.h"

#if ENABLE_ECHO_MODE
void echo_handler(reply_builder_t *rb);
#endif

#endif
