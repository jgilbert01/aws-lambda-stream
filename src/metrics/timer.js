class Timer {
  constructor({ start, last, checkpoints }) {
    this.start = (new Date(start || Timer.now())).getTime();
    this.last = last || this.start;
    this.checkpoints = { ...(checkpoints || {}) };
  }

  static now() {
    return Date.now();
  }

  checkpoint(key) {
    const now = Timer.now();
    this.checkpoints[key] = {
      value: now - this.last,
    };
    this.last = now;
    return this;
  }

  end(key) {
    const now = Timer.now();
    this.checkpoints[key] = {
      value: now - this.start,
    };
    this.last = now;
    return this;
  }
}

export default Timer;
