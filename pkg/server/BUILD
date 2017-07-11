package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = [
        "galley.go",
        "marshaler.go",
        "service.go",
        "util.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//api:galley/v1",
        "//pkg/store:go_default_library",
        "//pkg/store/inventory:go_default_library",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@org_golang_google_grpc//codes:go_default_library",
        "@org_golang_google_grpc//metadata:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
        "@org_golang_x_net//context:go_default_library",
    ],
)

go_test(
    name = "small_tests",
    srcs = [
        "marshaler_test.go",
        "service_test.go",
    ],
    library = ":go_default_library",
    deps = [
        "//pkg/store/memstore:go_default_library",
        "@com_github_ghodss_yaml//:go_default_library",
    ],
)
