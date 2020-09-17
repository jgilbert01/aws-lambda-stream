import { save, get } from '../../models/thing';

export const saveThing = (req, res) =>
  save(req.namespace, req.params.id, req.body)
    .then(data => res.status(200).json({}));

export const getThing = (req, res) =>
  get(req.namespace, req.params.id)
    .then(data => res.status(200).json(data));
