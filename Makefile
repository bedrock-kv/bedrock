TARGET_OS ?= $(shell uname -s)
ERTS_INCLUDE_DIR ?= $(shell erl -noshell -eval 'io:format("~s", [code:root_dir() ++ "/erts-" ++ erlang:system_info(version) ++ "/include"]), halt().')
MIX_APP_PATH ?= _build/$(MIX_ENV)/lib/bedrock
NATIVE_OUTPUT = $(MIX_APP_PATH)/priv/local_filesystem_mutation.so
CFLAGS ?= -O2
NATIVE_FLAGS = -std=c11 -fPIC -Wall -Wextra -Werror -I$(ERTS_INCLUDE_DIR)
ifeq ($(TARGET_OS),Darwin)
SHARED_FLAGS = -dynamiclib -undefined dynamic_lookup
else ifeq ($(TARGET_OS),Linux)
SHARED_FLAGS = -shared
else
$(error LocalFilesystem native mutation supports Linux and Darwin only; TARGET_OS=$(TARGET_OS))
endif
.PHONY: all clean native_directory
all: $(NATIVE_OUTPUT)
# Mix can initially symlink build/priv to source/priv. Replace only that link,
# keeping native artifacts specific to this build/target rather than the source.
native_directory:
	@if test -L "$(MIX_APP_PATH)/priv"; then rm "$(MIX_APP_PATH)/priv"; fi
	mkdir -p "$(MIX_APP_PATH)/priv"
	cp -R priv/schemas "$(MIX_APP_PATH)/priv/"
$(NATIVE_OUTPUT): c_src/local_filesystem_mutation.c Makefile native_directory
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(NATIVE_FLAGS) $(SHARED_FLAGS) $< $(LDFLAGS) -o $@
clean:
	rm -f $(NATIVE_OUTPUT)
