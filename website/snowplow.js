import ExecutionEnvironment from "@docusaurus/ExecutionEnvironment";
import { newTracker, trackPageView } from "@snowplow/browser-tracker";

const isProduction = process.env.NODE_ENV === "production";
const trackerConfig = {
    appId: "karapace-docs",
    platform: "web",
    forceSecureTracker: true,
    discoverRootDomain: true,
    cookieSameSite: "Lax",
    anonymousTracking: { withServerAnonymisation: true },
    postPath: "/aiven/dc2",
    crossDomainLinker: function (linkElement) {
        return linkElement.id === "crossDomainLink";
    },
    stateStorageStrategy: "none",
    eventMethod: "post",
    contexts: {
        webPage: true,
    },
};

function setupBrowserTracker() {
    newTracker("at", "dc.aiven.io", trackerConfig);
}

if (isProduction && ExecutionEnvironment.canUseDOM) {
    // only set tracker on prod
    setupBrowserTracker();
}

const module = {
    onRouteDidUpdate({ location, previousLocation }) {
        // only set tracker on prod
        if (isProduction) {
            // only call trackPageView when page route changed
            if (location.pathname !== previousLocation?.pathname) {
                // see https://github.com/facebook/docusaurus/pull/7424 regarding setTimeout
                setTimeout(() => {
                    trackPageView();
                });
            }
        }
    },
};

export default module;
