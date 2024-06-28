class Timer {
  constructor({ start, last, checkpoints }) {
    this.start = (new Date(start)).getTime();
    this.last = last || this.start;
    this.checkpoints = { ...(checkpoints || {}) };
  }

  checkpoint(key) {
    const now = Date.now();
    this.checkpoints[key] = {
      value: now - this.last,
    };
    this.last = now;
    return this;
  }

  end(key) {
    const now = Date.now();
    this.checkpoints[key] = {
      value: now - this.start,
    };
    this.last = now;
    return this;
  }
}

export default Timer;
