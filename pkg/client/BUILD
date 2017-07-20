load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["client.go"],
    visibility = ["//visibility:public"],
    deps = [
        "//api:galley/v1",  # keep
        "//pkg/client/config:go_default_library",
        "//pkg/client/file:go_default_library",
        "@com_github_golang_glog//:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = ["client_test.go"],
    library = ":go_default_library",
    deps = [
        "//pkg/client/config:go_default_library",
        "//pkg/client/file:go_default_library",
        "//pkg/server:go_default_library",
        "//pkg/store:go_default_library",
        "//pkg/store/memstore:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)
