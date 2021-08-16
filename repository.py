import abc
import model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, batch):
        # self.session.execute('INSERT INTO ??
        self.session.execute(
            'INSERT INTO batches (reference, sku, _purchased_quantity, eta) '
            'VALUES (:reference, :sku, :quantity, :eta)',
            dict(reference=batch.reference, sku=batch.sku, quantity=batch._purchased_quantity, eta=batch.eta)
        )

        batch_id = self.get_batch_id(batch.reference)

        for item in batch._allocations:
            self.session.execute(
                'INSERT INTO order_lines (sku, qty, orderid)'
                ' VALUES (:sku, :qty, :orderid)',
                dict(sku=item.sku, qty=item.qty, orderid=item.orderid)
            )

            orderline_id = self.get_orderline_id(sku=item.sku, qty=item.qty, orderid=item.orderid)

            self.session.execute(
                'INSERT INTO allocations (orderline_id, batch_id)'
                ' VALUES (:orderline_id, :batch_id)',
                dict(orderline_id=orderline_id, batch_id=batch_id)
            )

    def get(self, reference) -> model.Batch:
        # self.session.execute('SELECT ??
        [[ref, sku, quantity, eta]] = self.session.execute(
            'SELECT reference, sku, _purchased_quantity, eta FROM batches where reference=:reference',
            dict(reference=reference)
        )

        allocations = self.get_allocations(ref)

        batch = model.Batch(
            ref=ref,
            sku=sku,
            qty=quantity,
            eta=eta
        )

        for orderid in allocations:
            orderline = self.get_orderline_item(orderid)
            batch.allocate(orderline)

        return batch

    def get_batch_id(self, reference) -> model.Batch:
        # self.session.execute('SELECT ??
        print(reference)
        l = self.session.execute(
            'SELECT id FROM batches WHERE reference=:reference',
            dict(reference=reference)
        )
        batch_id = None
        for row in l:
            batch_id = row[0]

        return batch_id

    def get_orderline_id(self, sku, qty, orderid) -> model.Batch:
        # self.session.execute('SELECT ??
        ids = self.session.execute(
            'SELECT id FROM order_lines where sku=:sku AND qty=:qty AND orderid=:orderid',
            dict(sku=sku, qty=qty, orderid=orderid)
        )

        order_id = None
        for item in ids:
            order_id = item[0]

        return order_id
    
    def get_orderline_item(self, orderid):
        [[sku, quantity, orderid]] = self.session.execute(
            'SELECT sku, qty, orderid FROM order_lines where orderid=:orderid',
            dict(orderid=orderid)
        )

        return model.OrderLine(
            orderid=orderid,
            sku=sku,
            qty=quantity
        )

    def get_allocations(self, batchid):
        rows = list(
            self.session.execute(
                "SELECT orderid"
                " FROM allocations"
                " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                " JOIN batches ON allocations.batch_id = batches.id"
                " WHERE batches.reference = :batchid",
                dict(batchid=batchid),
            )
        )

        return {row[0] for row in rows}