import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowRight,
  CheckCircle2,
  Citrus,
  Github,
  LoaderCircle,
  LogOut,
  ShieldCheck,
  Sparkles,
} from "lucide-react";

const STORAGE_KEY = "kumquat-early-access-signup";
const GOOGLE_START_URL = "/api/auth/google/start";
const AUTH_ME_URL = "/api/auth/me";
const AUTH_LOGOUT_URL = "/api/auth/logout";
const AUTH_EXCHANGE_URL = "/api/auth/google/exchange";

function getCurrentPath() {
  return window.location.pathname.replace(/\/+$/, "") || "/";
}

async function readJson(response) {
  try {
    return await response.json();
  } catch {
    return {};
  }
}

function AppShell({ children }) {
  return (
    <main className="page-shell">
      <div className="background-grid" />
      <div className="background-sun" />
      <div className="background-orb orb-left" />
      <div className="background-orb orb-right" />
      {children}
    </main>
  );
}

function SiteMark() {
  return (
    <p className="eyebrow">
      <Sparkles size={16} strokeWidth={2.2} />
      Kumquat
    </p>
  );
}

function AuthSummary({ auth, onLogout }) {
  if (auth.status === "loading") {
    return (
      <div className="account-card">
        <p className="account-label">Identity</p>
        <p className="account-copy">Checking session...</p>
      </div>
    );
  }

  if (auth.user) {
    return (
      <div className="account-card">
        <p className="account-label">Signed In</p>
        <h3>{auth.user.full_name}</h3>
        <p className="account-copy">{auth.user.email}</p>
        <div className="account-badges">
          <span className="mini-pill">
            <ShieldCheck size={14} />
            Google verified
          </span>
        </div>
        <button className="secondary-button" onClick={onLogout} type="button">
          <LogOut size={16} />
          Sign out
        </button>
      </div>
    );
  }

  return (
    <div className="account-card">
      <p className="account-label">Identity</p>
      <h3>Custom sign-in flow</h3>
      <p className="account-copy">
        Google handles consent. Kumquat owns every page before and after it.
      </p>
      <a className="secondary-button" href="/auth/sign-in">
        Sign in
        <ArrowRight size={16} />
      </a>
    </div>
  );
}

function HomePage({ auth, onLogout }) {
  const [formData, setFormData] = useState({ name: "", email: "" });
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");

  useEffect(() => {
    const savedSignup = window.localStorage.getItem(STORAGE_KEY);
    if (!savedSignup) {
      return;
    }

    try {
      const parsedSignup = JSON.parse(savedSignup);
      setFormData({
        name: parsedSignup.name ?? "",
        email: parsedSignup.email ?? "",
      });
      setStatus("success");
      setMessage("You're on the early access list.");
    } catch {
      window.localStorage.removeItem(STORAGE_KEY);
    }
  }, []);

  useEffect(() => {
    if (!auth.user) {
      return;
    }

    setFormData((current) => ({
      name: current.name || auth.user.full_name || "",
      email: current.email || auth.user.email || "",
    }));
  }, [auth.user]);

  function handleChange(event) {
    const { name, value } = event.target;
    setFormData((current) => ({ ...current, [name]: value }));
    if (status !== "idle") {
      setStatus("idle");
      setMessage("");
    }
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setStatus("loading");
    setMessage("");

    try {
      const response = await fetch("/api/early-access", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(formData),
      });

      const data = await readJson(response);
      if (!response.ok) {
        throw new Error(data.error || "Signup failed.");
      }

      window.localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          name: data.signup.name,
          email: data.signup.email,
        }),
      );
      setFormData({
        name: data.signup.name,
        email: data.signup.email,
      });
      setStatus("success");
      setMessage("You're on the early access list.");
    } catch (error) {
      setStatus("error");
      setMessage(error.message || "Signup failed.");
    }
  }

  return (
    <AppShell>
      <section className="hero-panel">
        <div className="hero-copy">
          <SiteMark />
          <h1>Bright login, no borrowed auth pages.</h1>
          <p className="supporting-copy">
            Kumquat now ships a custom Google sign-in flow across the frontend and
            Django backend, while keeping consent and identity verification where it
            belongs.
          </p>

          <div className="action-row">
            {auth.user ? (
              <div className="status-pill">
                <CheckCircle2 size={16} />
                Signed in as {auth.user.first_name || auth.user.email}
              </div>
            ) : (
              <a className="primary-link" href="/auth/sign-in">
                <Citrus size={18} />
                Sign in with Google
                <ArrowRight size={18} />
              </a>
            )}
            <a
              className="ghost-link"
              href="https://github.com/kumquatben/kumquat"
              rel="noreferrer"
              target="_blank"
            >
              <Github size={18} />
              View on GitHub
            </a>
          </div>

          <div className="feature-grid">
            <article className="feature-panel">
              <p className="feature-kicker">Custom Experience</p>
              <p className="feature-text">
                Sign-in, callback, loading, error, and signed-in states all live in
                Kumquat UI instead of generic provider screens.
              </p>
            </article>
            <article className="feature-panel">
              <p className="feature-kicker">Django Session Auth</p>
              <p className="feature-text">
                Google identity is exchanged server-side and persisted with Django's
                built-in auth system.
              </p>
            </article>
          </div>
        </div>

        <aside className="right-rail">
          <AuthSummary auth={auth} onLogout={onLogout} />

          <form className="signup-form" onSubmit={handleSubmit}>
            <div className="signup-header">
              <p className="signup-label">Early Access Signup</p>
              <p className="signup-copy">
                Join the list and the backend will keep the record synced.
              </p>
            </div>

            <div className="signup-fields">
              <label className="signup-field">
                <span>Name</span>
                <input
                  autoComplete="name"
                  name="name"
                  onChange={handleChange}
                  placeholder="Kumquat fan"
                  type="text"
                  value={formData.name}
                />
              </label>
              <label className="signup-field">
                <span>Email</span>
                <input
                  autoComplete="email"
                  name="email"
                  onChange={handleChange}
                  placeholder="you@example.com"
                  required
                  type="email"
                  value={formData.email}
                />
              </label>
            </div>

            <div className="signup-actions">
              <button className="signup-button" disabled={status === "loading"} type="submit">
                {status === "loading" ? <LoaderCircle className="spinner" size={18} /> : null}
                {status === "success" ? "Signed up" : "Join early access"}
              </button>
              {message ? (
                <p className={`signup-message signup-message-${status}`}>{message}</p>
              ) : null}
            </div>
          </form>
        </aside>
      </section>
    </AppShell>
  );
}

function SignInPage({ auth }) {
  return (
    <AppShell>
      <section className="auth-layout">
        <div className="auth-copy-panel">
          <SiteMark />
          <h1 className="auth-title">Sign in without losing the plot.</h1>
          <p className="supporting-copy">
            Kumquat uses Google only for identity. The product pages, loading states,
            and account surface stay fully custom.
          </p>
          <div className="auth-notes">
            <div className="auth-note">
              <span className="note-index">01</span>
              <p>Custom frontend entry page</p>
            </div>
            <div className="auth-note">
              <span className="note-index">02</span>
              <p>Django handles token exchange and session login</p>
            </div>
            <div className="auth-note">
              <span className="note-index">03</span>
              <p>Users land back on a Kumquat callback screen</p>
            </div>
          </div>
        </div>

        <div className="auth-card">
          <p className="auth-card-label">Google Sign-In</p>
          <h2>{auth.user ? `You're already in, ${auth.user.first_name || auth.user.full_name}.` : "Use your Google account"}</h2>
          <p className="auth-card-copy">
            Continue with Google and Kumquat will create or reuse your Django user
            account automatically.
          </p>
          {auth.user ? (
            <a className="primary-link wide-button" href="/">
              Back to home
              <ArrowRight size={18} />
            </a>
          ) : (
            <a className="google-button" href={GOOGLE_START_URL}>
              <span className="google-badge">G</span>
              Continue with Google
              <ArrowRight size={18} />
            </a>
          )}
          <a className="text-link" href="/">
            Return home
          </a>
        </div>
      </section>
    </AppShell>
  );
}

function CallbackPage({ onAuthResolved }) {
  const params = useMemo(() => new URLSearchParams(window.location.search), []);
  const hasStartedRef = useRef(false);
  const [status, setStatus] = useState("loading");
  const [message, setMessage] = useState("Finishing your Kumquat session...");
  const [resolvedUser, setResolvedUser] = useState(null);

  useEffect(() => {
    if (hasStartedRef.current) {
      return;
    }
    hasStartedRef.current = true;

    async function completeGoogleAuth() {
      const error = params.get("error");
      if (error) {
        setStatus("error");
        setMessage("Google sign-in was canceled or denied.");
        return;
      }

      const code = params.get("code");
      const state = params.get("state");
      if (!code || !state) {
        setStatus("error");
        setMessage("Missing OAuth parameters from Google.");
        return;
      }

      try {
        const response = await fetch(AUTH_EXCHANGE_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ code, state }),
        });
        const data = await readJson(response);
        if (!response.ok) {
          throw new Error(data.error || "Google sign-in failed.");
        }

        setResolvedUser(data.user);
        onAuthResolved(data.user);
        setStatus("success");
        setMessage(`Signed in as ${data.user.full_name}.`);
        window.history.replaceState({}, "", "/auth/google/callback");
      } catch (errorObject) {
        setStatus("error");
        setMessage(errorObject.message || "Google sign-in failed.");
      }
    }

    completeGoogleAuth();
  }, [onAuthResolved, params]);

  return (
    <AppShell>
      <section className="callback-card">
        <SiteMark />
        <div className={`callback-icon callback-icon-${status}`}>
          {status === "loading" ? (
            <LoaderCircle className="spinner" size={28} />
          ) : status === "success" ? (
            <CheckCircle2 size={28} />
          ) : (
            <ShieldCheck size={28} />
          )}
        </div>
        <h1 className="auth-title">
          {status === "loading"
            ? "Authenticating..."
            : status === "success"
              ? "You're in."
              : "Sign-in didn't finish."}
        </h1>
        <p className="callback-copy">{message}</p>
        <div className="callback-actions">
          <a className="primary-link wide-button" href="/">
            {resolvedUser ? "Continue home" : "Back to home"}
            <ArrowRight size={18} />
          </a>
          {status === "error" ? (
            <a className="secondary-button" href="/auth/sign-in">
              Try again
            </a>
          ) : null}
        </div>
      </section>
    </AppShell>
  );
}

export default function App() {
  const [auth, setAuth] = useState({ status: "loading", user: null });
  const path = getCurrentPath();

  useEffect(() => {
    let active = true;

    async function loadSession() {
      try {
        const response = await fetch(AUTH_ME_URL);
        const data = await readJson(response);
        if (!active) {
          return;
        }
        setAuth({
          status: "ready",
          user: data.authenticated ? data.user : null,
        });
      } catch {
        if (!active) {
          return;
        }
        setAuth({ status: "ready", user: null });
      }
    }

    loadSession();
    return () => {
      active = false;
    };
  }, []);

  async function handleLogout() {
    await fetch(AUTH_LOGOUT_URL, { method: "POST" });
    setAuth({ status: "ready", user: null });
    window.location.assign("/");
  }

  function handleAuthResolved(user) {
    setAuth({ status: "ready", user });
  }

  if (path === "/auth/sign-in") {
    return <SignInPage auth={auth} />;
  }

  if (path === "/auth/google/callback") {
    return <CallbackPage onAuthResolved={handleAuthResolved} />;
  }

  return <HomePage auth={auth} onLogout={handleLogout} />;
}
