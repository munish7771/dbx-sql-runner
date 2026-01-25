import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

const FeatureList = [
  {
    title: 'Minimalist & SQL-First',
    Svg: require('@site/static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Just write <code>.sql</code> files. No complex boilerplate or excessive configuration.
        Designed for <strong>Databricks SQL</strong> where Unity Catalog already handles metadata and lineage.
      </>
    ),
  },
  {
    title: 'Library-First Design',
    Svg: require('@site/static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        Not just a CLI. Import <code>dbx_sql_runner</code> in your Python scripts.
        Perfect for orchestrating transformations within <strong>Airflow</strong> or <strong>Databricks Jobs</strong>.
      </>
    ),
  },
  {
    title: 'Built-in Tooling',
    Svg: require('@site/static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Comes with a built-in <strong>Linter</strong> (via Ruff) to enforce code quality and
        <strong>Alerting</strong> webhooks to notify you of run status.
      </>
    ),
  },
];

function Feature({ Svg, title, description }) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        {typeof Svg === 'string' ? (
          <img src={Svg} className={styles.featureSvg} role="img" alt={title} />
        ) : (
          <Svg className={styles.featureSvg} role="img" />
        )}
      </div>
      <div className="text--center padding-horiz--md">
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
