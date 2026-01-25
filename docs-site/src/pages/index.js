import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import HomepageFeatures from '@site/src/components/HomepageFeatures';

import Heading from '@theme/Heading';
import styles from './index.module.css';

function HomepageHeader() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <Heading as="h1" className="hero__title">
          {siteConfig.title}
        </Heading>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get Started 🚀
          </Link>
        </div>
      </div>
    </header>
  );
}

function WhySection() {
  return (
    <div className={clsx('container', styles.sectionContainer)} style={{ padding: '4rem 0', textAlign: 'center' }}>
      <Heading as="h2">Why dbx-sql-runner?</Heading>
      <div className="row" style={{ justifyContent: 'center', marginTop: '2rem' }}>
        <div className="col col--8">
          <p style={{ fontSize: '1.2rem' }}>
            <strong>DBT is powerful, but often overkill for modern Databricks.</strong>
          </p>
          <p className="text--left">
            When you're running on Databricks with Unity Catalog, you already have:
          </p>
          <ul className="text--left" style={{ display: 'inline-block', textAlign: 'left' }}>
            <li>✅ Built-in Metadata Management (Unity Catalog)</li>
            <li>✅ Automatic Data Lineage</li>
            <li>✅ Robust SQL support</li>
          </ul>
          <p className="text--left" style={{ marginTop: '1rem' }}>
            <strong>dbx-sql-runner</strong> strips away the complexity. It gives you just what you need:
            a lightweight DAG runner for your SQL files, environment management, and essential tooling like
            linting and alerting. No heavy adapters, no jinja-hell, just SQL.
          </p>
        </div>
      </div>
    </div>
  );
}

export default function Home() {
  const { siteConfig } = useDocusaurusContext();
  return (
    <Layout
      title={`Home`}
      description="A lightweight SQL runner for Databricks">
      <HomepageHeader />
      <main>
        <WhySection />
        <HomepageFeatures />
      </main>
    </Layout>
  );
}
