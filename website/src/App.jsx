import { ArrowUpRight, Github, Sparkles, SunMedium } from "lucide-react";

export default function App() {
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
