const assert = require("assert");
const slugid = require("slugid");
const { UNIQUE_VIOLATION } = require("taskcluster-lib-postgres");
const { getEntries } = require("../utils");

class FakeGithub {
  constructor() {
    this.taskclusterGithubBuilds = new Map();
    this.taskclusterIntegrationOwners = new Map();
    this.taskclusterChecksToTasks = new Map();
    this.taskclusterCheckRuns = new Map();
    this.githubBuilds = new Map();
  }

  /* helpers */

  reset() {
    this.taskclusterGithubBuilds = new Map();
    this.taskclusterIntegrationOwners = new Map();
    this.taskclusterChecksToTasks = new Map();
    this.taskclusterCheckRuns = new Map();
    this.githubBuilds = new Map();
  }

  _getTaskclusterGithubBuild({ partitionKey, rowKey }) {
    return this.taskclusterGithubBuilds.get(`${partitionKey}-${rowKey}`);
  }

  _removeTaskclusterGithubBuild({ partitionKey, rowKey }) {
    this.taskclusterGithubBuilds.delete(`${partitionKey}-${rowKey}`);
  }

  _addTaskclusterGithubBuild(taskclusterGithubBuild) {
    assert(typeof taskclusterGithubBuild.partition_key === "string");
    assert(typeof taskclusterGithubBuild.row_key === "string");
    assert(typeof taskclusterGithubBuild.value === "object");
    assert(typeof taskclusterGithubBuild.version === "number");

    const etag = slugid.v4();
    const c = {
      partition_key_out: taskclusterGithubBuild.partition_key,
      row_key_out: taskclusterGithubBuild.row_key,
      value: taskclusterGithubBuild.value,
      version: taskclusterGithubBuild.version,
      etag,
    };

    this.taskclusterGithubBuilds.set(`${c.partition_key_out}-${c.row_key_out}`, c);

    return c;
  }

  _getTaskclusterIntegrationOwner({ partitionKey, rowKey }) {
    return this.taskclusterIntegrationOwners.get(`${partitionKey}-${rowKey}`);
  }

  _removeTaskclusterIntegrationOwner({ partitionKey, rowKey }) {
    this.taskclusterIntegrationOwners.delete(`${partitionKey}-${rowKey}`);
  }

  _addTaskclusterIntegrationOwner(taskclusterIntegrationOwner) {
    assert(typeof taskclusterIntegrationOwner.partition_key === "string");
    assert(typeof taskclusterIntegrationOwner.row_key === "string");
    assert(typeof taskclusterIntegrationOwner.value === "object");
    assert(typeof taskclusterIntegrationOwner.version === "number");

    const etag = slugid.v4();
    const c = {
      partition_key_out: taskclusterIntegrationOwner.partition_key,
      row_key_out: taskclusterIntegrationOwner.row_key,
      value: taskclusterIntegrationOwner.value,
      version: taskclusterIntegrationOwner.version,
      etag,
    };

    this.taskclusterIntegrationOwners.set(`${c.partition_key_out}-${c.row_key_out}`, c);

    return c;
  }

  /* fake functions */

  async taskcluster_github_builds_entities_load(partitionKey, rowKey) {
    const taskclusterGithubBuild = this._getTaskclusterGithubBuild({ partitionKey, rowKey });

    return taskclusterGithubBuild ? [taskclusterGithubBuild] : [];
  }

  async taskcluster_github_builds_entities_create(partition_key, row_key, value, overwrite, version) {
    if (!overwrite && this._getTaskclusterGithubBuild({ partitionKey: partition_key, rowKey: row_key })) {
      const err = new Error("duplicate key value violates unique constraint");
      err.code = UNIQUE_VIOLATION;
      throw err;
    }

    const taskclusterGithubBuild = this._addTaskclusterGithubBuild({
      partition_key,
      row_key,
      value,
      version,
    });

    return [{ "taskcluster_github_builds_entities_create": taskclusterGithubBuild.etag }];
  }

  async taskcluster_github_builds_entities_remove(partition_key, row_key) {
    const taskclusterGithubBuild = this._getTaskclusterGithubBuild({ partitionKey: partition_key, rowKey: row_key });
    this._removeTaskclusterGithubBuild({ partitionKey: partition_key, rowKey: row_key });

    return taskclusterGithubBuild ? [{ etag: taskclusterGithubBuild.etag }] : [];
  }

  async taskcluster_github_builds_entities_modify(partition_key, row_key, value, version, oldEtag) {
    const taskclusterGithubBuild = this._getTaskclusterGithubBuild({ partitionKey: partition_key, rowKey: row_key });

    if (!taskclusterGithubBuild) {
      const err = new Error("no such row");
      err.code = "P0002";
      throw err;
    }

    if (taskclusterGithubBuild.etag !== oldEtag) {
      const err = new Error("unsuccessful update");
      err.code = "P0004";
      throw err;
    }

    const c = this._addTaskclusterGithubBuild({ partition_key, row_key, value, version });
    return [{ etag: c.etag }];
  }

  async taskcluster_github_builds_entities_scan(partition_key, row_key, condition, size, offset) {
    const entries = getEntries({
      partitionKey: partition_key,
      rowKey: row_key,
      condition,
    }, this.taskclusterGithubBuilds);

    return entries.slice(offset, offset + size + 1);
  }

  async taskcluster_integration_owners_entities_load(partitionKey, rowKey) {
    const taskclusterIntegrationOwner = this._getTaskclusterIntegrationOwner({ partitionKey, rowKey });

    return taskclusterIntegrationOwner ? [taskclusterIntegrationOwner] : [];
  }

  async taskcluster_integration_owners_entities_create(partition_key, row_key, value, overwrite, version) {
    if (!overwrite && this._getTaskclusterIntegrationOwner({ partitionKey: partition_key, rowKey: row_key })) {
      const err = new Error("duplicate key value violates unique constraint");
      err.code = UNIQUE_VIOLATION;
      throw err;
    }

    const taskclusterIntegrationOwner = this._addTaskclusterIntegrationOwner({
      partition_key,
      row_key,
      value,
      version,
    });

    return [{ "taskcluster_integration_owners_entities_create": taskclusterIntegrationOwner.etag }];
  }

  async taskcluster_integration_owners_entities_remove(partition_key, row_key) {
    const taskclusterIntegrationOwner = this._getTaskclusterIntegrationOwner({
      partitionKey: partition_key,
      rowKey: row_key,
    });
    this._removeTaskclusterIntegrationOwner({ partitionKey: partition_key, rowKey: row_key });

    return taskclusterIntegrationOwner ? [{ etag: taskclusterIntegrationOwner.etag }] : [];
  }

  async taskcluster_integration_owners_entities_modify(partition_key, row_key, value, version, oldEtag) {
    const taskclusterIntegrationOwner = this._getTaskclusterIntegrationOwner({
      partitionKey: partition_key,
      rowKey: row_key,
    });

    if (!taskclusterIntegrationOwner) {
      const err = new Error("no such row");
      err.code = "P0002";
      throw err;
    }

    if (taskclusterIntegrationOwner.etag !== oldEtag) {
      const err = new Error("unsuccessful update");
      err.code = "P0004";
      throw err;
    }

    const c = this._addTaskclusterIntegrationOwner({ partition_key, row_key, value, version });
    return [{ etag: c.etag }];
  }

  async taskcluster_integration_owners_entities_scan(partition_key, row_key, condition, size, offset) {
    const entries = getEntries({
      partitionKey: partition_key,
      rowKey: row_key,
      condition,
    }, this.taskclusterIntegrationOwners);

    return entries.slice(offset, offset + size + 1);
  }

  _getTaskclusterChecksToTask({ partitionKey, rowKey }) {
    return this.taskclusterChecksToTasks.get(`${partitionKey}-${rowKey}`);
  }

  _removeTaskclusterChecksToTask({ partitionKey, rowKey }) {
    this.taskclusterChecksToTasks.delete(`${partitionKey}-${rowKey}`);
  }

  _addTaskclusterChecksToTask(taskclusterChecksToTask) {
    assert(typeof taskclusterChecksToTask.partition_key === "string");
    assert(typeof taskclusterChecksToTask.row_key === "string");
    assert(typeof taskclusterChecksToTask.value === "object");
    assert(typeof taskclusterChecksToTask.version === "number");

    const etag = slugid.v4();
    const c = {
      partition_key_out: taskclusterChecksToTask.partition_key,
      row_key_out: taskclusterChecksToTask.row_key,
      value: taskclusterChecksToTask.value,
      version: taskclusterChecksToTask.version,
      etag,
    };

    this.taskclusterChecksToTasks.set(`${c.partition_key_out}-${c.row_key_out}`, c);

    return c;
  }

  _getTaskclusterCheckRun({ partitionKey, rowKey }) {
    return this.taskclusterCheckRuns.get(`${partitionKey}-${rowKey}`);
  }

  _removeTaskclusterCheckRun({ partitionKey, rowKey }) {
    this.taskclusterCheckRuns.delete(`${partitionKey}-${rowKey}`);
  }

  _addTaskclusterCheckRun(taskclusterCheckRun) {
    assert(typeof taskclusterCheckRun.partition_key === "string");
    assert(typeof taskclusterCheckRun.row_key === "string");
    assert(typeof taskclusterCheckRun.value === "object");
    assert(typeof taskclusterCheckRun.version === "number");

    const etag = slugid.v4();
    const c = {
      partition_key_out: taskclusterCheckRun.partition_key,
      row_key_out: taskclusterCheckRun.row_key,
      value: taskclusterCheckRun.value,
      version: taskclusterCheckRun.version,
      etag,
    };

    this.taskclusterCheckRuns.set(`${c.partition_key_out}-${c.row_key_out}`, c);

    return c;
  }

  /* fake functions */

  async taskcluster_checks_to_tasks_entities_load(partitionKey, rowKey) {
    const taskclusterChecksToTask = this._getTaskclusterChecksToTask({ partitionKey, rowKey });

    return taskclusterChecksToTask ? [taskclusterChecksToTask] : [];
  }

  async taskcluster_checks_to_tasks_entities_create(partition_key, row_key, value, overwrite, version) {
    if (!overwrite && this._getTaskclusterChecksToTask({ partitionKey: partition_key, rowKey: row_key })) {
      const err = new Error("duplicate key value violates unique constraint");
      err.code = UNIQUE_VIOLATION;
      throw err;
    }

    const taskclusterChecksToTask = this._addTaskclusterChecksToTask({
      partition_key,
      row_key,
      value,
      version,
    });

    return [{ "taskcluster_checks_to_tasks_entities_create": taskclusterChecksToTask.etag }];
  }

  async taskcluster_checks_to_tasks_entities_remove(partition_key, row_key) {
    const taskclusterChecksToTask = this._getTaskclusterChecksToTask({ partitionKey: partition_key, rowKey: row_key });
    this._removeTaskclusterChecksToTask({ partitionKey: partition_key, rowKey: row_key });

    return taskclusterChecksToTask ? [{ etag: taskclusterChecksToTask.etag }] : [];
  }

  async taskcluster_checks_to_tasks_entities_modify(partition_key, row_key, value, version, oldEtag) {
    const taskclusterChecksToTask = this._getTaskclusterChecksToTask({ partitionKey: partition_key, rowKey: row_key });

    if (!taskclusterChecksToTask) {
      const err = new Error("no such row");
      err.code = "P0002";
      throw err;
    }

    if (taskclusterChecksToTask.etag !== oldEtag) {
      const err = new Error("unsuccessful update");
      err.code = "P0004";
      throw err;
    }

    const c = this._addTaskclusterChecksToTask({ partition_key, row_key, value, version });
    return [{ etag: c.etag }];
  }

  async taskcluster_checks_to_tasks_entities_scan(partition_key, row_key, condition, size, offset) {
    const entries = getEntries({
      partitionKey: partition_key,
      rowKey: row_key,
      condition,
    }, this.taskclusterChecksToTasks);

    return entries.slice(offset, offset + size + 1);
  }

  async taskcluster_check_runs_entities_load(partitionKey, rowKey) {
    const taskclusterCheckRun = this._getTaskclusterCheckRun({ partitionKey, rowKey });

    return taskclusterCheckRun ? [taskclusterCheckRun] : [];
  }

  async taskcluster_check_runs_entities_create(partition_key, row_key, value, overwrite, version) {
    if (!overwrite && this._getTaskclusterCheckRun({ partitionKey: partition_key, rowKey: row_key })) {
      const err = new Error("duplicate key value violates unique constraint");
      err.code = UNIQUE_VIOLATION;
      throw err;
    }

    const taskclusterCheckRun = this._addTaskclusterCheckRun({
      partition_key,
      row_key,
      value,
      version,
    });

    return [{ "taskcluster_check_runs_entities_create": taskclusterCheckRun.etag }];
  }

  async taskcluster_check_runs_entities_remove(partition_key, row_key) {
    const taskclusterCheckRun = this._getTaskclusterCheckRun({ partitionKey: partition_key, rowKey: row_key });
    this._removeTaskclusterCheckRun({ partitionKey: partition_key, rowKey: row_key });

    return taskclusterCheckRun ? [{ etag: taskclusterCheckRun.etag }] : [];
  }

  async taskcluster_check_runs_entities_modify(partition_key, row_key, value, version, oldEtag) {
    const taskclusterCheckRun = this._getTaskclusterCheckRun({ partitionKey: partition_key, rowKey: row_key });

    if (!taskclusterCheckRun) {
      const err = new Error("no such row");
      err.code = "P0002";
      throw err;
    }

    if (taskclusterCheckRun.etag !== oldEtag) {
      const err = new Error("unsuccessful update");
      err.code = "P0004";
      throw err;
    }

    const c = this._addTaskclusterCheckRun({ partition_key, row_key, value, version });
    return [{ etag: c.etag }];
  }

  async taskcluster_check_runs_entities_scan(partition_key, row_key, condition, size, offset) {
    const entries = getEntries({ partitionKey: partition_key, rowKey: row_key, condition }, this.taskclusterCheckRuns);

    return entries.slice(offset, offset + size + 1);
  }

  async get_github_build(worker_pool_id) {
    const taskclusterGithubBuild = this.githubBuilds.get(worker_pool_id);

    return taskclusterGithubBuild ? [taskclusterGithubBuild] : [];
  }

  async get_github_builds(page_size, page_offset, organization, repository, sha) {
    assert(organization !== undefined);
    assert(repository !== undefined);
    assert(sha !== undefined);
    const builds = [...this.githubBuilds.values()];
    builds.sort((a, b) => a.updated - b.updated);
    return builds
      .filter(b => {
        if (organization && b.organization !== organization) {
          return false;
        }
        if (repository && b.repository !== repository) {
          return false;
        }
        if (sha && b.sha !== sha) {
          return false;
        }
        return true;
      })
      .slice(page_offset || 0, page_size ? page_offset + page_size : builds.length);
  }

  async delete_github_build(task_group_id) {
    this.githubBuilds.delete(task_group_id);
  }

  async create_github_build(organization, repository, sha, task_group_id, state,
    created, updated, installation_id, event_type, event_id) {
    assert.equal(typeof organization, 'string');
    assert.equal(typeof repository, 'string');
    assert.equal(typeof sha, 'string');
    assert.equal(typeof task_group_id, 'string');
    assert.equal(typeof state, 'string');
    assert(created.toTimeString); // duck type a date
    assert(updated.toTimeString); // duck type a date
    assert.equal(typeof installation_id, 'number');
    assert.equal(typeof event_type, 'string');
    assert.equal(typeof event_id, 'string');

    if (this.githubBuilds.get(task_group_id)) {
      const error = new Error('duplicate key value violates unique constraint');
      error.code = UNIQUE_VIOLATION;
      throw error;
    }

    this.githubBuilds.set(task_group_id, {
      organization,
      repository,
      sha,
      task_group_id,
      state,
      created,
      updated,
      installation_id,
      event_type,
      event_id,
      etag: slugid.v4(),
    });
  }

  async set_github_build_state(task_group_id, state) {
    assert.equal(typeof task_group_id, 'string');
    assert.equal(typeof state, 'string');

    const old = this.githubBuilds.get(task_group_id);
    if (!old) {
      const error = new Error('no such row');
      error.code = 'P0002';
      throw error;
    }

    const updated_row = {
      ...old,
      state,
      updated: new Date(),
      etag: slugid.v4(),
    };
    this.githubBuilds.set(task_group_id, updated_row);
  }
}

module.exports = FakeGithub;
