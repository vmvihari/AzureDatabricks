# Lesson 7.2: Monitoring and Alerting

## Table of Contents
- [The Importance of Observability](#the-importance-of-observability)
- [Azure Monitor and Log Analytics](#azure-monitor-and-log-analytics)
- [Action Step: Configuring Workflow Alerts](#action-step-configuring-workflow-alerts)
- [Interview Preparation](#interview-preparation)

---

## The Importance of Observability

When you deploy a Databricks Workflow to run at 3:00 AM, you cannot physically monitor it. If the `bronze_ingestion` task fails due to a corrupt CSV file, you need to know immediately.

In a production Lakehouse, **Observability** is mandatory. If a pipeline fails silently, the BI Dashboards will show stale data the next morning, eroding trust in the Data Engineering team.

---

## Azure Monitor and Log Analytics

At an enterprise level, you do not just rely on Databricks internal logs. Databricks workspaces are typically integrated with **Azure Log Analytics** (via Azure Monitor).

Every time a cluster starts, a query executes, or a job fails, Databricks pushes a diagnostic log to Azure Log Analytics. Centralizing logs allows the DevOps team to set up cross-platform alerting (e.g., if Databricks fails AND Azure Data Factory fails, trigger a high-priority PagerDuty incident).

---

## 🛠️ Action Step: Configuring Workflow Alerts

While Azure Monitor handles the enterprise logs, Databricks provides native, simple email and webhook alerting directly inside the Workflow JSON.

Let's ensure our `mortgage_daily_etl` job alerts the team if anything goes wrong.

1. Open `apps/mortgage-data-platform/deploy/mortgage_workflow.json`.
2. Add an `email_notifications` block at the root level of the JSON (outside the `tasks` array).

```json
{
  "name": "mortgage_daily_etl",
  "email_notifications": {
    "on_failure": ["data-engineering-alerts@mortgagecorp.com"],
    "no_alert_for_skipped_runs": true
  },
  "job_clusters": [
    // ... cluster config ...
  ],
  "tasks": [
    // ... tasks ...
  ]
}
```
*(Note: You can also use Webhooks to send alerts directly to a Slack or Microsoft Teams channel).*

---

## 🎯 Interview Preparation

> [!TIP]
> **Q1: If a Databricks Workflow fails at 3:00 AM, how do you ensure the engineering team is notified and has the logs necessary to debug it?**
> **Answer:** "I configure native failure alerting within the Databricks Workflow JSON via the `email_notifications` or `webhook_notifications` blocks, which instantly alerts the on-call engineer via PagerDuty or Slack. For debugging, I ensure the Databricks workspace is integrated with Azure Log Analytics. This centralizes the diagnostic settings and cluster logs. By querying the Log Analytics workspace, the engineer can immediately view the stack trace and executor metrics to determine if the failure was a code exception or an Out Of Memory (OOM) spill."

---
[⬅️ Previous: Lesson 7.1: Databricks Workflows](lesson-7.1-databricks-workflows.md) | [🏠 Main Directory](../../README.md) | [➡️ Next: Lesson 7.3: Git and CI/CD](lesson-7.3-git-and-cicd.md)
