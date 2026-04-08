import { useEffect, useState } from "react";
import {
  ArrowUpRight,
  Github,
  LoaderCircle,
  Sparkles,
  SunMedium,
} from "lucide-react";

const STORAGE_KEY = "kumquat-early-access-signup";

export default function App() {
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

      const data = await response.json();

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
    <main className="page-shell">
      <div className="background-orb orb-left" />
      <div className="background-orb orb-right" />

      <section className="hero-panel">
        <div className="hero-copy">
          <p className="eyebrow">
            <Sparkles size={16} strokeWidth={2.2} />
            Kumquat
          </p>
          <h1>A small citrus with unreasonable confidence.</h1>
          <p className="supporting-copy">
            Kumquat is a bright little web project with a sharper peel, a softer
            center, and just enough polish to feel deliberate.
          </p>

          <form className="signup-form" onSubmit={handleSubmit}>
            <div className="signup-header">
              <p className="signup-label">Early Access Signup</p>
              <p className="signup-copy">
                Join the list and we&apos;ll keep this page honest as the product
                catches up.
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

          <div className="action-row">
            <a
              className="primary-link"
              href="https://github.com/kumquatben/kumquat"
              target="_blank"
              rel="noreferrer"
            >
              <Github size={18} />
              View on GitHub
              <ArrowUpRight size={18} />
            </a>
            <p className="status-pill">
              <SunMedium size={16} />
              Live at kumquat.info
            </p>
          </div>
        </div>

        <aside className="feature-card">
          <div className="fruit-mark" aria-hidden="true">
            <div className="fruit-glow" />
            <div className="fruit-core" />
            <div className="fruit-leaf" />
          </div>

          <div className="feature-grid">
            <article>
              <p className="feature-label">Tone</p>
              <p className="feature-value">Warm, crisp, citrus-forward.</p>
            </article>
            <article>
              <p className="feature-label">Stack</p>
              <p className="feature-value">React, Vite, Docker, ECR, k3s.</p>
            </article>
            <article>
              <p className="feature-label">Delivery</p>
              <p className="feature-value">Public HTTPS with ALB ingress.</p>
            </article>
          </div>
        </aside>
      </section>
    </main>
  );
}
