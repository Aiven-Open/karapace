const { writeFileSync } = require("fs");

/**
 * function create a file with all links
 * this is a temp plugin to help us restructure our pages
 * without missing any links that need redirects
 * @param {object} props - the `props` object returned from postBuild
 */
function createLinkFile(props) {
    const routes = JSON.stringify(props.routesPaths, null, 2);
    const filePath = "./current-routes.json";

    // Write the routes to the file
    writeFileSync(filePath, routes, "utf-8");

    console.log(`Routes paths have been saved to ${filePath}`);
}

export { createLinkFile };
