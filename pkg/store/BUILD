package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "builder.go",
        "store.go",
    ],
    deps = [
    ],
)

go_test(
    name = "small_tests",
    size = "small",
    srcs = [
        "builder_test.go",
    ],
    library = ":go_default_library",
    deps = [
        ":go_default_library",
    ],
)
