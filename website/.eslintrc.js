module.exports = {
    root: true,
    env: {
        commonjs: true,
        browser: true,
        es2021: true,
        node: true,
    },
    extends: [
        "eslint:recommended",
        "plugin:@docusaurus/recommended",
        "plugin:json/recommended-legacy",
        "plugin:react/recommended",
        "prettier",
    ],
    parserOptions: {
        ecmaFeatures: {
            jsx: true,
        },
        ecmaVersion: 12,
        sourceType: "module",
    },
    plugins: ["json", "react"],
    settings: {
        react: {
            version: "detect",
        },
    },
    ignorePatterns: ["build", ".docusaurus", ".vale"],
};
