import { save, get, del } from '../../models/thing';

export const getThing = (req, res) => get(req.namespace, req.params.id)
  .then((data) => res.status(200).json(data));

export const saveThing = (req, res) => save(req.namespace, req.params.id, req.body)
  .then((data) => res.status(200).json({}));

export const deleteThing = (req, res) => del(req.namespace, req.params.id)
  .then((data) => res.status(200).json({}));
