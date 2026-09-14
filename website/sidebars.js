/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
    docs: [
        "index",
        {
            type: "category",
            label: "Get started",
            items: ["install", "api-examples"],
        },
        {
            type: "category",
            label: "Reference",
            items: ["configuration", "authentication", "compatibility"],
        },
        {
            type: "category",
            label: "Operations",
            items: ["deployment", "observability", "troubleshooting"],
        },
    ],
};

module.exports = sidebars;
