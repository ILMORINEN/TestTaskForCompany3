using System;

namespace TestTaskForCompany3
{
    public class Deal
    {
        public string sellerName { get; set; } = "";

        public string sellerInn { get; set; } = "";

        public string buyerName { get; set; } = "";

        public string buyerInn { get; set; } = "";

        public decimal woodVolumeBuyer { get; set; } = default;

        public decimal woodVolumeSeller { get; set; } = default;

        public DateTime dealDate { get; set; }

        public string dealNumber { get; set; } = "";

        internal bool isValid()
        {
            if (sellerName.Length > 1000)
                return false;

            if (sellerInn.Length != 12 && sellerInn.Length != 10)
                return false;

            if (buyerName.Length > 1000)
                return false;

            if (buyerInn.Length != 12 && buyerInn.Length != 10)
                return false;

            if (woodVolumeBuyer < 0)
                return false;

            if (woodVolumeSeller < 0)
                return false;

            if (dealDate == null || dealDate < new DateTime(1753, 1, 1, 0, 0, 0) || dealDate > new DateTime(9999, 12, 31, 23, 59, 59))
                return false;

            if (dealNumber == null || dealNumber.Length != 28)
                return false;

            return true;
        }
    }
}