module.exports = class ApiError extends Error {
  constructor(apiErrDef) {
    super(apiErrDef.message);
    this.name = apiErrDef.name;
    this.code = apiErrDef.code;
    this.retry = apiErrDef.retry;
  }
}
