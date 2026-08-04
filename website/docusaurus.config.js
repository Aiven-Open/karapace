// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion
const {
    addRedirectsFile,
} = require("./scripts/post-build-plugins/add-redirects-file");
const {
    createLinkFile,
} = require("./scripts/post-build-plugins/create-link-file");
const lightCodeTheme = require("prism-react-renderer").themes.github;
const darkCodeTheme = require("prism-react-renderer").themes.nightOwl;

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: "Karapace",
    tagline:
        "A free and Open Source tool serving all your Apache Kafka® HTTP needs",
    favicon: "images/favicon.png",

    url: "https://www.karapace.io",
    baseUrl: "/",

    organizationName: "Aiven-Open",
    projectName: "karapace",

    onBrokenLinks: "throw",

    markdown: {
        hooks: {
            onBrokenMarkdownLinks: "warn",
        },
    },

    clientModules: [require.resolve("./snowplow.js")],

    i18n: {
        defaultLocale: "en",
        locales: ["en"],
    },

    presets: [
        [
            "classic",
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    sidebarPath: require.resolve("./sidebars.js"),
                },
                blog: false,
                theme: {
                    customCss: require.resolve("./src/css/custom.css"),
                },
            }),
        ],
    ],
    themeConfig:
        /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            colorMode: {
                defaultMode: "light",
                disableSwitch: false,
                respectPrefersColorScheme: true,
            },
            image: "images/karapace-dark-mode.svg",
            navbar: {
                title: "Karapace",
                logo: {
                    alt: "Karapace",
                    src: "images/karapace-light-mode.svg",
                    srcDark: "images/karapace-dark-mode.svg",
                },
                items: [
                    {
                        type: "docSidebar",
                        sidebarId: "docs",
                        position: "left",
                        label: "Docs",
                    },
                    {
                        href: "https://github.com/Aiven-Open/karapace",
                        label: "GitHub",
                        position: "right",
                    },
                ],
            },
            footer: {
                style: "dark",
                links: [
                    {
                        title: "Documentation",
                        items: [
                            { label: "Get started", to: "/docs/install" },
                            { label: "API examples", to: "/docs/api-examples" },
                            {
                                label: "Configuration",
                                to: "/docs/configuration",
                            },
                        ],
                    },
                    {
                        title: "Community",
                        items: [
                            {
                                label: "GitHub",
                                href: "https://github.com/Aiven-Open/karapace",
                            },
                            {
                                label: "Issues",
                                href: "https://github.com/Aiven-Open/karapace/issues",
                            },
                        ],
                    },
                    {
                        title: "More",
                        items: [
                            {
                                label: "Aiven",
                                href: "https://aiven.io/",
                            },
                        ],
                    },
                ],
                copyright: `Copyright © ${new Date().getFullYear()} Aiven. Apache Kafka® is either a registered trademark or a trademark of the Apache Software Foundation in the United States and/or other countries.`,
            },
            prism: {
                theme: lightCodeTheme,
                darkTheme: darkCodeTheme,
                additionalLanguages: ["bash", "json", "protobuf"],
            },
        }),
    themes: [
        [
            require.resolve("@easyops-cn/docusaurus-search-local"),
            // Offline, client-side search. Builds the index at build time; no
            // external service, no cookies.
            {
                hashed: true,
                indexBlog: false,
                docsRouteBasePath: "/docs",
                highlightSearchTermsOnTargetPage: true,
            },
        ],
    ],
    plugins: [
        () => ({
            name: "custom-plugin",
            postBuild(props) {
                createLinkFile(props);
                addRedirectsFile(props);
            },
        }),
    ],
};

module.exports = config;
