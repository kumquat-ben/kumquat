import { useEffect, useMemo, useRef, useState } from "react";
import {
  AnimatePresence,
  motion,
  useAnimationControls,
  useAnimationFrame,
  useInView,
  useMotionValue,
  useReducedMotion,
} from "framer-motion";
import {
  ArrowRight,
  CheckCircle2,
  LoaderCircle,
  LogOut,
  ShieldCheck,
} from "lucide-react";

const STORAGE_KEY = "kumquat-early-access-signup";
const NAV_PULSE_KEY = "kumquat-nav-cta-pulsed";
const GOOGLE_START_URL = "/api/auth/google/start";
const AUTH_ME_URL = "/api/auth/me";
const AUTH_LOGOUT_URL = "/api/auth/logout";
const AUTH_EXCHANGE_URL = "/api/auth/google/exchange";
const ADMIN_DASHBOARD_URL = "/api/admin/dashboard";

const BILL_ITEMS = [
  { label: "$100", kind: "bill", id: "KMQ-00100000" },
  { label: "$50", kind: "bill", id: "KMQ-00050000" },
  { label: "$20", kind: "bill", id: "KMQ-00020000" },
  { label: "$10", kind: "bill", id: "KMQ-00010000" },
  { label: "$5", kind: "bill", id: "KMQ-00005000" },
  { label: "$1", kind: "bill", id: "KMQ-00001000" },
  { label: "$0.50", kind: "coin", id: "KMQ-00000500" },
  { label: "$0.25", kind: "coin", id: "KMQ-00000250" },
  { label: "$0.10", kind: "coin", id: "KMQ-00000100" },
  { label: "$0.05", kind: "coin", id: "KMQ-00000050" },
  { label: "$0.01", kind: "coin", id: "KMQ-00000010" },
];

const HOW_IT_WORKS_STEPS = [
  {
    number: "01",
    title: "Mint cash as objects",
    body: "Kumquat treats each denomination as a discrete unit with a visible identity, so value feels held rather than abstracted away into a balance row.",
  },
  {
    number: "02",
    title: "Send it like handing over cash",
    body: "Transfers read like passing something real from one wallet to another. Motion reinforces the handoff instead of decorating it.",
  },
  {
    number: "03",
    title: "Read the wallet at a glance",
    body: "The interface surfaces denomination mix, object count, and total in the same view so the mental model stays physical and legible.",
  },
];

const DENOMINATION_GRID = [
  { label: "$100", type: "bill" },
  { label: "$50", type: "bill" },
  { label: "$20", type: "bill" },
  { label: "$10", type: "bill" },
  { label: "$5", type: "bill" },
  { label: "$1", type: "bill" },
  { label: "$0.50", type: "coin" },
  { label: "$0.25", type: "coin" },
  { label: "$0.10", type: "coin" },
  { label: "$0.05", type: "coin" },
  { label: "$0.01", type: "coin" },
];

const WALLET_ROWS = [
  { label: "$100.00", kind: "bill", detail: "Treasury note unit", amount: 100.0 },
  { label: "$50.00", kind: "bill", detail: "Reserve transfer", amount: 50.0 },
  { label: "$20.00", kind: "bill", detail: "Payroll cash object", amount: 20.0 },
  { label: "$10.00", kind: "bill", detail: "Merchant settlement", amount: 10.0 },
  { label: "$5.00", kind: "bill", detail: "Retail change unit", amount: 5.0 },
  { label: "$1.00", kind: "bill", detail: "Pocket balance", amount: 1.0 },
  { label: "$0.25", kind: "coin", detail: "Coin remainder", amount: 0.25 },
  { label: "$0.10", kind: "coin", detail: "Coin remainder", amount: 0.1 },
  { label: "$0.01", kind: "coin", detail: "Coin remainder", amount: 0.01 },
];

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

function formatCurrency(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
  }).format(value);
}

function AppShell({ children }) {
  return (
    <main className="app-shell">
      <div className="page-noise" />
      <div className="page-orbit orbit-one" />
      <div className="page-orbit orbit-two" />
      {children}
    </main>
  );
}

function SiteMark() {
  return (
    <a className="site-mark" href="#top">
      <span className="site-mark-dot" />
      Kumquat
    </a>
  );
}

function AuthSummary({ auth, onLogout }) {
  if (auth.status === "loading") {
    return (
      <div className="identity-card">
        <p className="section-eyebrow">Identity</p>
        <p className="body-copy">Checking session...</p>
      </div>
    );
  }

  if (auth.user) {
    return (
      <div className="identity-card">
        <p className="section-eyebrow">Signed In</p>
        <h3>{auth.user.full_name}</h3>
        <p className="body-copy">{auth.user.email}</p>
        <div className="pill-row">
          <span className="meta-pill">
            <ShieldCheck size={14} />
            Google verified
          </span>
          {auth.user.is_superuser ? <span className="meta-pill">Superuser</span> : null}
        </div>
        {auth.user.is_superuser ? (
          <a className="button button-secondary button-block" href="/admin/dashboard">
            Admin dashboard
            <ArrowRight size={16} />
          </a>
        ) : null}
        <button className="button button-secondary" onClick={onLogout} type="button">
          <LogOut size={16} />
          Sign out
        </button>
      </div>
    );
  }

  return (
    <div className="identity-card">
      <p className="section-eyebrow">Identity</p>
      <h3>Custom sign-in flow</h3>
      <p className="body-copy">
        Google handles consent. Kumquat keeps the product story, loading states, and
        account surface on-brand.
      </p>
      <a className="button button-secondary" href="/auth/sign-in">
        Sign in
        <ArrowRight size={16} />
      </a>
    </div>
  );
}

function NavBar({ ctaControls }) {
  return (
    <motion.header
      animate={{ opacity: 1, y: 0 }}
      className="site-nav"
      id="top"
      initial={{ opacity: 0, y: -16 }}
      transition={{ duration: 0.45 }}
    >
      <SiteMark />
      <nav className="nav-links" aria-label="Primary">
        <a href="#story">Story</a>
        <a href="#how-it-works">How it works</a>
        <a href="#denominations">Denominations</a>
      </nav>
      <motion.a
        animate={ctaControls}
        className="button button-nav"
        href="https://github.com/kumquatben/kumquat"
        rel="noreferrer"
        target="_blank"
      >
        View on GitHub
      </motion.a>
    </motion.header>
  );
}

function HeroSection({ auth }) {
  const prefersReducedMotion = useReducedMotion();

  return (
    <section className="hero-section">
      <div className="hero-decor">
        <div className="coin-outline coin-large" />
        <div className="coin-outline coin-medium" />
        <div className="coin-outline coin-small" />
      </div>
      <motion.p
        animate={{ opacity: 1, y: 0 }}
        className="hero-eyebrow"
        initial={{ opacity: 0, y: -12 }}
        transition={{ duration: 0.5, delay: prefersReducedMotion ? 0 : 0.05 }}
      >
        Physical cash mental model for the internet
      </motion.p>
      <motion.h1
        animate={{ opacity: 1, y: 0 }}
        className="hero-title"
        initial={{ opacity: 0, y: -18 }}
        transition={{
          delay: prefersReducedMotion ? 0 : 0.12,
          type: prefersReducedMotion ? "tween" : "spring",
          stiffness: 180,
          damping: 18,
        }}
      >
        Money that behaves like
        {" "}
        <em>objects you can hold.</em>
      </motion.h1>
      <motion.p
        animate={{ opacity: 1, y: 0 }}
        className="hero-copy"
        initial={{ opacity: 0, y: 18 }}
        transition={{ duration: 0.55, delay: prefersReducedMotion ? 0 : 0.2 }}
      >
        Kumquat Chain turns denominations into visible units with weight, hierarchy,
        and transfer logic that reads like handing someone real cash.
      </motion.p>
      <motion.div
        animate={{ opacity: 1, y: 0 }}
        className="hero-actions"
        initial={{ opacity: 0, y: 16 }}
        transition={{ duration: 0.45, delay: prefersReducedMotion ? 0 : 0.3 }}
      >
        <a className="button button-primary" href={auth.user ? "#how-it-works" : "/auth/sign-in"}>
          {auth.user ? "See the wallet model" : "Sign in with Google"}
          <ArrowRight size={16} />
        </a>
        <a className="button button-secondary" href="#story">
          Read the story
        </a>
      </motion.div>
    </section>
  );
}

function BillsStrip() {
  const prefersReducedMotion = useReducedMotion();
  const trackRef = useRef(null);
  const firstLoopRef = useRef(null);
  const x = useMotionValue(0);
  const [loopWidth, setLoopWidth] = useState(0);
  const [hoveredId, setHoveredId] = useState(null);
  const currentSpeedRef = useRef(1);
  const targetSpeedRef = useRef(1);

  useEffect(() => {
    function updateWidth() {
      setLoopWidth(firstLoopRef.current?.offsetWidth || 0);
    }

    updateWidth();
    window.addEventListener("resize", updateWidth);
    return () => window.removeEventListener("resize", updateWidth);
  }, []);

  useAnimationFrame((_, delta) => {
    if (prefersReducedMotion || loopWidth === 0) {
      return;
    }

    currentSpeedRef.current += (targetSpeedRef.current - currentSpeedRef.current) * 0.08;
    const next = x.get() - (delta / 1000) * 62 * currentSpeedRef.current;

    if (Math.abs(next) >= loopWidth) {
      x.set(0);
      return;
    }

    x.set(next);
  });

  const loops = [0, 1];

  return (
    <section className="bills-strip" aria-label="Denomination strip">
      <div
        className="bills-viewport"
        onMouseEnter={() => {
          targetSpeedRef.current = 0.15;
        }}
        onMouseLeave={() => {
          targetSpeedRef.current = 1;
          setHoveredId(null);
        }}
        ref={trackRef}
      >
        <motion.div className="bills-track" style={{ x }}>
          {loops.map((loop) => (
            <div
              className="bills-loop"
              key={loop}
              ref={loop === 0 ? firstLoopRef : null}
            >
              {BILL_ITEMS.map((bill, index) => (
                <motion.div
                  animate={{ opacity: 1, y: 0 }}
                  className="bill-card"
                  initial={{ opacity: 0, y: -24 }}
                  key={`${loop}-${bill.id}`}
                  onHoverEnd={() => setHoveredId(null)}
                  onHoverStart={() => setHoveredId(bill.id)}
                  transition={{
                    delay: prefersReducedMotion || loop === 1 ? 0 : index * 0.06,
                    type: prefersReducedMotion ? "tween" : "spring",
                    stiffness: 280,
                    damping: 22,
                  }}
                  whileHover={prefersReducedMotion ? undefined : { scale: 1.06, y: -3 }}
                >
                  <span className="bill-value">{bill.label}</span>
                  <span className="bill-kind">{bill.kind}</span>
                  {hoveredId === bill.id ? (
                    <span className="bill-tooltip">{bill.id}</span>
                  ) : null}
                </motion.div>
              ))}
            </div>
          ))}
        </motion.div>
      </div>
    </section>
  );
}

function StorySection({ auth, onLogout, formData, handleChange, handleSubmit, message, status }) {
  const isSignedUp = status === "success";

  return (
    <section className="story-section" id="story">
      <div className="story-copy-panel">
        <p className="section-eyebrow">General Direction</p>
        <h2 className="section-title">
          Make the chain feel
          {" "}
          <em>tactile, calm, and exact.</em>
        </h2>
        <p className="body-copy">
          The page should feel editorial rather than startup-generic. Typography does
          the talking. Color stays citrus-warm and restrained. Motion exists to teach
          the model: denominations are things, not just numbers.
        </p>
        <div className="story-principles">
          <article>
            <p className="principle-index">01</p>
            <h3>Explain through motion</h3>
            <p className="body-copy">
              Use springs and sequenced reveals to show hierarchy, transfer, and object
              arrival. If animation does not clarify behavior, remove it.
            </p>
          </article>
          <article>
            <p className="principle-index">02</p>
            <h3>Keep the interface physically legible</h3>
            <p className="body-copy">
              Bills, coins, trays, and wallet rows need visible boundaries, subtle
              lift, and enough spacing to read as units with weight.
            </p>
          </article>
        </div>
      </div>

      <div className="story-side-panel">
        <AuthSummary auth={auth} onLogout={onLogout} />

        <section className={`signup-card ${isSignedUp ? "signup-card-success" : ""}`}>
          <form className="signup-form" onSubmit={handleSubmit}>
            <div className="signup-head">
              <p className="section-eyebrow">Early Access</p>
              <p className="body-copy">
                Join the list and keep the product rollout grounded in real users.
              </p>
            </div>

            <label className="field">
              <span>Name</span>
              <input
                autoComplete="name"
                name="name"
                onChange={handleChange}
                placeholder="Kumquat user"
                type="text"
                value={formData.name}
              />
            </label>
            <label className="field">
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

            <div className="signup-actions">
              <button className="button button-primary button-block" disabled={status === "loading"} type="submit">
                {status === "loading" ? <LoaderCircle className="spinner" size={16} /> : null}
                Join early access
              </button>
              {message && !isSignedUp ? (
                <p className={`signup-message signup-message-${status}`}>{message}</p>
              ) : null}
            </div>
          </form>

          <div aria-live="polite" className="signup-success" role="status">
            <div className="signup-success-icon">
              <CheckCircle2 size={20} />
            </div>
            <p className="section-eyebrow">Saved</p>
            <h3>You’re on the list.</h3>
            <p className="body-copy">
              We stored your early-access signup and will reach out when the next
              release is ready.
            </p>
          </div>
        </section>
      </div>
    </section>
  );
}

function AnimatedStep({ step }) {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, amount: 0.6 });

  return (
    <motion.li className="step-item" ref={ref}>
      <motion.span
        animate={inView ? { opacity: 1 } : { opacity: 0 }}
        className="step-number"
        transition={{ duration: 0.2 }}
      >
        {step.number}
      </motion.span>
      <motion.span
        animate={inView ? { opacity: 1, scaleX: 1 } : { opacity: 0, scaleX: 0 }}
        className="step-line"
        transition={{ duration: 0.25, delay: 0.15 }}
      />
      <div className="step-content">
        <motion.h3
          animate={inView ? { opacity: 1, x: 0 } : { opacity: 0, x: 8 }}
          transition={{ duration: 0.25, delay: 0.3 }}
        >
          {step.title}
        </motion.h3>
        <motion.p
          animate={inView ? { opacity: 1 } : { opacity: 0 }}
          className="body-copy"
          transition={{ duration: 0.28, delay: 0.42 }}
        >
          {step.body}
        </motion.p>
      </div>
    </motion.li>
  );
}

function WalletCard() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, amount: 0.4 });
  const [displayTotal, setDisplayTotal] = useState(0);
  const total = WALLET_ROWS.reduce((sum, row) => sum + row.amount, 0);

  useEffect(() => {
    if (!inView) {
      return;
    }

    let frameId = 0;
    let startTime = 0;
    const duration = 600;

    function tick(timestamp) {
      if (!startTime) {
        startTime = timestamp;
      }

      const elapsed = timestamp - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - (1 - progress) ** 3;
      setDisplayTotal(total * eased);

      if (progress < 1) {
        frameId = window.requestAnimationFrame(tick);
      }
    }

    frameId = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frameId);
  }, [inView, total]);

  return (
    <div className="wallet-card" ref={ref}>
      <div className="wallet-card-head">
        <p className="section-eyebrow">Wallet Preview</p>
        <p className="wallet-total">{formatCurrency(displayTotal)}</p>
      </div>
      <div className="wallet-rows">
        {WALLET_ROWS.map((row, index) => (
          <motion.div
            animate={inView ? { opacity: 1, x: 0 } : { opacity: 0, x: 40 }}
            className="wallet-row"
            initial={false}
            key={`${row.label}-${index}`}
            transition={{
              delay: index * 0.07,
              type: "spring",
              stiffness: 320,
              damping: 24,
            }}
          >
            <div>
              <p className="wallet-row-value">{row.label}</p>
              <p className="wallet-row-detail">{row.detail}</p>
            </div>
            <motion.span
              animate={inView ? { opacity: 1, scale: 1 } : { opacity: 0, scale: 0.6 }}
              className={`wallet-badge wallet-badge-${row.kind}`}
              transition={{
                delay: index * 0.07 + 0.08,
                type: "spring",
                stiffness: 320,
                damping: 22,
              }}
            >
              {row.kind}
            </motion.span>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

function HowItWorksSection() {
  return (
    <section className="how-section" id="how-it-works">
      <div className="how-copy">
        <p className="section-eyebrow">How It Works</p>
        <h2 className="section-title">
          Show the transfer model
          {" "}
          <em>one beat at a time.</em>
        </h2>
        <ol className="steps-list">
          {HOW_IT_WORKS_STEPS.map((step) => (
            <AnimatedStep key={step.number} step={step} />
          ))}
        </ol>
      </div>
      <WalletCard />
    </section>
  );
}

function DenominationGridSection() {
  const ref = useRef(null);
  const inView = useInView(ref, { once: true, amount: 0.2 });

  return (
    <section className="denomination-section" id="denominations" ref={ref}>
      <p className="section-eyebrow">Denominations</p>
      <h2 className="section-title">
        Bills and coins should feel like a
        {" "}
        <em>tray of units.</em>
      </h2>
      <div className="denomination-grid">
        {DENOMINATION_GRID.map((item, index) => (
          <motion.article
            animate={inView ? { opacity: 1, y: 0 } : { opacity: 0, y: 16 }}
            className={`denomination-card denomination-card-${item.type}`}
            key={item.label}
            transition={{ duration: 0.35, delay: index * 0.04 }}
            whileHover={{
              y: -6,
              boxShadow: "0 12px 32px rgba(26, 18, 8, 0.10)",
              rotateX: 3,
            }}
          >
            <p className="denomination-kind">{item.type}</p>
            <motion.p
              className="denomination-value"
              whileHover={item.type === "coin" ? { scale: 1.04, color: "#d85a30" } : { color: "#d85a30" }}
            >
              {item.label}
            </motion.p>
          </motion.article>
        ))}
      </div>
    </section>
  );
}

function HomePage({ auth, onLogout }) {
  const [formData, setFormData] = useState({ name: "", email: "" });
  const [status, setStatus] = useState("idle");
  const [message, setMessage] = useState("");
  const ctaControls = useAnimationControls();

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

  useEffect(() => {
    if (window.sessionStorage.getItem(NAV_PULSE_KEY)) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      ctaControls.start({
        scale: [1, 1.07, 1],
        transition: { duration: 0.5, ease: "easeInOut" },
      });
      window.sessionStorage.setItem(NAV_PULSE_KEY, "true");
    }, 3000);

    return () => window.clearTimeout(timeoutId);
  }, [ctaControls]);

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
      <NavBar ctaControls={ctaControls} />
      <div className="home-page">
        <HeroSection auth={auth} />
        <BillsStrip />
        <StorySection
          auth={auth}
          formData={formData}
          handleChange={handleChange}
          handleSubmit={handleSubmit}
          message={message}
          onLogout={onLogout}
          status={status}
        />
        <HowItWorksSection />
        <DenominationGridSection />
      </div>
    </AppShell>
  );
}

function SignInPage({ auth }) {
  return (
    <AppShell>
      <section className="utility-layout">
        <div className="utility-copy">
          <p className="section-eyebrow">Google Sign-In</p>
          <h1 className="utility-title">
            Sign in without losing
            {" "}
            <em>the product voice.</em>
          </h1>
          <p className="body-copy">
            Kumquat uses Google only for identity. Every state before and after consent
            stays inside the product.
          </p>
          <div className="story-principles">
            <article>
              <p className="principle-index">01</p>
              <h3>Custom entry page</h3>
              <p className="body-copy">The app owns the pre-consent framing.</p>
            </article>
            <article>
              <p className="principle-index">02</p>
              <h3>Django session exchange</h3>
              <p className="body-copy">Identity resolves server-side and persists cleanly.</p>
            </article>
          </div>
        </div>

        <div className="utility-card">
          <p className="section-eyebrow">Continue</p>
          <h2 className="section-title section-title-compact">
            {auth.user ? `You're already in, ${auth.user.first_name || auth.user.full_name}.` : "Use your Google account"}
          </h2>
          <p className="body-copy">
            Continue with Google and Kumquat will create or reuse your Django user
            account automatically.
          </p>
          {auth.user ? (
            <a className="button button-primary button-block" href="/">
              Back to home
              <ArrowRight size={16} />
            </a>
          ) : (
            <a className="button button-primary button-block" href={GOOGLE_START_URL}>
              Continue with Google
              <ArrowRight size={16} />
            </a>
          )}
          <a className="button button-secondary button-block" href="/">
            Return home
          </a>
        </div>
      </section>
    </AppShell>
  );
}

function formatDate(value) {
  if (!value) {
    return "Never";
  }

  const parsedDate = new Date(value);
  if (Number.isNaN(parsedDate.getTime())) {
    return value;
  }

  return parsedDate.toLocaleString();
}

function paginateItems(items, page, pageSize) {
  const totalPages = Math.max(1, Math.ceil(items.length / pageSize));
  const safePage = Math.min(Math.max(page, 1), totalPages);
  const start = (safePage - 1) * pageSize;

  return {
    totalPages,
    safePage,
    items: items.slice(start, start + pageSize),
  };
}

function AdminDashboardPage({ auth }) {
  const [dashboard, setDashboard] = useState({ status: "loading", data: null, error: "" });
  const [activeTab, setActiveTab] = useState("signups");
  const [page, setPage] = useState(1);

  useEffect(() => {
    if (auth.status !== "ready") {
      return;
    }

    if (!auth.user?.is_superuser) {
      setDashboard({
        status: "error",
        data: null,
        error: auth.user ? "Superuser access required." : "You need to sign in first.",
      });
      return;
    }

    let active = true;

    async function loadDashboard() {
      try {
        const response = await fetch(ADMIN_DASHBOARD_URL);
        const data = await readJson(response);
        if (!active) {
          return;
        }
        if (!response.ok) {
          throw new Error(data.error || "Failed to load admin dashboard.");
        }
        setDashboard({ status: "ready", data, error: "" });
      } catch (errorObject) {
        if (!active) {
          return;
        }
        setDashboard({
          status: "error",
          data: null,
          error: errorObject.message || "Failed to load admin dashboard.",
        });
      }
    }

    loadDashboard();
    return () => {
      active = false;
    };
  }, [auth]);

  const stats = dashboard.data?.stats;
  const users = dashboard.data?.users ?? [];
  const signups = dashboard.data?.signups ?? [];
  const pageSize = activeTab === "signups" ? 8 : 6;
  const activeItems = activeTab === "signups" ? signups : users;
  const pagination = paginateItems(activeItems, page, pageSize);

  useEffect(() => {
    setPage(1);
  }, [activeTab, signups.length, users.length]);

  return (
    <AppShell>
      <section className="dashboard-layout">
        <div className="dashboard-hero">
          <p className="section-eyebrow">Admin</p>
          <h1 className="utility-title">Product release dashboard.</h1>
          <p className="body-copy">
            Review signed-in users and the early access list in one place.
          </p>
          <div className="stats-grid">
            <article className="stat-card">
              <p className="section-eyebrow">Users</p>
              <h3>{stats?.users ?? "..."}</h3>
            </article>
            <article className="stat-card">
              <p className="section-eyebrow">Superusers</p>
              <h3>{stats?.superusers ?? "..."}</h3>
            </article>
            <article className="stat-card">
              <p className="section-eyebrow">Signups</p>
              <h3>{stats?.signups ?? "..."}</h3>
            </article>
          </div>
        </div>

        <div className="dashboard-panel">
          <div className="dashboard-toolbar">
            <a className="button button-secondary" href="/">
              Back home
            </a>
          </div>

          {dashboard.status === "loading" ? (
            <p className="dashboard-message">Loading dashboard data...</p>
          ) : null}
          {dashboard.status === "error" ? (
            <p className="dashboard-message dashboard-message-error">{dashboard.error}</p>
          ) : null}

          {dashboard.status === "ready" ? (
            <section className="dashboard-card">
              <div className="dashboard-card-header">
                <div className="dashboard-tabs" role="tablist" aria-label="Dashboard data views">
                  <button
                    className={`dashboard-tab ${activeTab === "signups" ? "dashboard-tab-active" : ""}`}
                    onClick={() => setActiveTab("signups")}
                    role="tab"
                    aria-selected={activeTab === "signups"}
                    type="button"
                  >
                    Early signups
                  </button>
                  <button
                    className={`dashboard-tab ${activeTab === "users" ? "dashboard-tab-active" : ""}`}
                    onClick={() => setActiveTab("users")}
                    role="tab"
                    aria-selected={activeTab === "users"}
                    type="button"
                  >
                    Users
                  </button>
                </div>
                <p className="body-copy">{activeItems.length} total</p>
              </div>
              <div className="data-table-wrap">
                <table className="data-table">
                  <thead>
                    <tr>
                      {activeTab === "signups" ? (
                        <>
                          <th>Name</th>
                          <th>Email</th>
                          <th>Created</th>
                        </>
                      ) : (
                        <>
                          <th>Name</th>
                          <th>Email</th>
                          <th>Role</th>
                          <th>Joined</th>
                          <th>Last Login</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {activeTab === "signups"
                      ? pagination.items.map((signup) => (
                          <tr key={signup.email}>
                            <td>{signup.name || "Unknown"}</td>
                            <td>{signup.email}</td>
                            <td>{formatDate(signup.created_at)}</td>
                          </tr>
                        ))
                      : pagination.items.map((user) => (
                          <tr key={user.username}>
                            <td>{user.full_name}</td>
                            <td>{user.email || user.username}</td>
                            <td>{user.is_superuser ? "Superuser" : user.is_staff ? "Staff" : "User"}</td>
                            <td>{formatDate(user.date_joined)}</td>
                            <td>{formatDate(user.last_login)}</td>
                          </tr>
                        ))}
                  </tbody>
                </table>
              </div>
              <div className="dashboard-pagination">
                <p className="body-copy">
                  Page {pagination.safePage} of {pagination.totalPages}
                </p>
                <div className="dashboard-pagination-actions">
                  <button
                    className="button button-secondary"
                    disabled={pagination.safePage === 1}
                    onClick={() => setPage((current) => Math.max(1, current - 1))}
                    type="button"
                  >
                    Previous
                  </button>
                  <button
                    className="button button-secondary"
                    disabled={pagination.safePage === pagination.totalPages}
                    onClick={() => setPage((current) => Math.min(pagination.totalPages, current + 1))}
                    type="button"
                  >
                    Next
                  </button>
                </div>
              </div>
            </section>
          ) : null}
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
      <section className="utility-layout utility-layout-single">
        <div className="utility-card utility-card-centered">
          <p className="section-eyebrow">Authentication</p>
          <div className={`callback-icon callback-icon-${status}`}>
            {status === "loading" ? (
              <LoaderCircle className="spinner" size={28} />
            ) : status === "success" ? (
              <CheckCircle2 size={28} />
            ) : (
              <ShieldCheck size={28} />
            )}
          </div>
          <h1 className="utility-title">
            {status === "loading"
              ? "Authenticating..."
              : status === "success"
                ? "You're in."
                : "Sign-in didn't finish."}
          </h1>
          <p className="body-copy">{message}</p>
          <div className="callback-actions">
            <a className="button button-primary button-block" href="/">
              {resolvedUser ? "Continue home" : "Back to home"}
              <ArrowRight size={16} />
            </a>
            {status === "error" ? (
              <a className="button button-secondary button-block" href="/auth/sign-in">
                Try again
              </a>
            ) : null}
          </div>
        </div>
      </section>
    </AppShell>
  );
}

function RoutedPage({ auth, handleAuthResolved, handleLogout, path }) {
  if (path === "/auth/sign-in") {
    return <SignInPage auth={auth} />;
  }

  if (path === "/auth/google/callback") {
    return <CallbackPage onAuthResolved={handleAuthResolved} />;
  }

  if (path === "/admin/dashboard") {
    return <AdminDashboardPage auth={auth} />;
  }

  return <HomePage auth={auth} onLogout={handleLogout} />;
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

  return (
    <AnimatePresence mode="wait">
      <motion.div
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: -20 }}
        initial={{ opacity: 0, y: 30 }}
        key={path}
        transition={{
          duration: 0.3,
          ease: [0, 0, 0.2, 1],
        }}
      >
        <RoutedPage
          auth={auth}
          handleAuthResolved={handleAuthResolved}
          handleLogout={handleLogout}
          path={path}
        />
      </motion.div>
    </AnimatePresence>
  );
}
