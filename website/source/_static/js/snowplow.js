(function (p, l, o, w, i, n, g) {
  if (!p[i]) {
    p.GlobalSnowplowNamespace = p.GlobalSnowplowNamespace || [];
    p.GlobalSnowplowNamespace.push(i);
    p[i] = function () {
      (p[i].q = p[i].q || []).push(arguments);
    };
    p[i].q = p[i].q || [];
    n = l.createElement(o);
    g = l.getElementsByTagName(o)[0];
    n.async = 1;
    n.src = w;
    g.parentNode.insertBefore(n, g);
  }
})(
  window,
  document,
  "script",
  "https://storage.googleapis.com/aiven-dw-prod-snowplow-tracker/3.4.0/gh7rnaha.js",
  "snowplow"
);

snowplow("newTracker", "at", "dc.aiven.io", {
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
});

snowplow("trackPageView");
